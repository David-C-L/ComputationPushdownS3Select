#include <cpprest/http_listener.h>
#include <cpprest/uri.h>
#include <cpprest/json.h>
#include <cpprest/producerconsumerstream.h>
#include <duckdb.hpp>
#include <fstream>
#include <sstream>
#include <regex>
#include <vector>
#include <string>
#include <memory>
#include <cstdio>
#include <iomanip>
#include <filesystem>

using namespace web;
using namespace web::http;
using namespace web::http::experimental::listener;

std::string generate_random_filename() {
    // Use the system temporary directory
    std::string temp_dir = std::filesystem::temp_directory_path().string();

    // Generate a random suffix for the file name
    auto now = std::chrono::system_clock::now();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

    std::random_device rd;
    std::mt19937 generator(rd());
    std::uniform_int_distribution<int> dist(100000, 999999);

    std::ostringstream filename;
    filename << temp_dir << "/duckdb_output_" << timestamp << "_" << dist(generator) << ".csv";

    return filename.str();
}

// Function to read a specific byte range from a file
std::vector<uint8_t> read_byte_range(const std::string& filepath, size_t start, size_t end) {
  std::ifstream file(filepath, std::ios::binary);
  if (!file) {
    throw std::runtime_error("Failed to open file: " + filepath);
  }

  // Determine file size
  file.seekg(0, std::ios::end);
  size_t file_size = file.tellg();
  file.seekg(0, std::ios::beg);

  // Adjust end range if it exceeds the file size
  if (end > file_size) {
    end = file_size;
  }

  // Read the requested byte range
  size_t range_size = end - start;
  file.seekg(start, std::ios::beg);
  std::vector<uint8_t> buffer(range_size);
  file.read(reinterpret_cast<char*>(buffer.data()), range_size);
  return buffer;
}

// Function to parse multiple ranges from the Range header
std::vector<std::pair<size_t, size_t>> parse_ranges(const std::string& range_header) {
  std::vector<std::pair<size_t, size_t>> ranges;
  std::regex range_regex(R"(bytes=(\d+)-(\d+))");
  std::smatch match;

  std::string::const_iterator search_start(range_header.cbegin());
  while (std::regex_search(search_start, range_header.cend(), match, range_regex)) {
    size_t start = std::stoul(match[1]);
    size_t end = std::stoul(match[2]) + 1; // Exclusive range
    ranges.emplace_back(start, end);
    search_start = match.suffix().first;
  }

  return ranges;
}

// Function to handle incoming requests asynchronously
void handle_request_byte_ranges(http_request request) {
  try {
    std::string filepath = "/home/david/Documents/PhD/datasets/parquet_tpch/tpch_1000MB_lineitem_gzip.parquet"; // Path to your file
    std::string range_header;

    // Check if the Range header is present
    if (request.headers().has(U("Range"))) {
      range_header = request.headers()[U("Range")];
    } else {
      request.reply(status_codes::RangeNotSatisfiable, "Missing or invalid Range header");
      return;
    }

    // Parse the Range header for multiple byte ranges
    auto ranges = parse_ranges(range_header);
    if (ranges.empty()) {
      request.reply(status_codes::RangeNotSatisfiable, "No valid ranges provided");
      return;
    }

    // Prepare the multipart/byteranges response
    std::string boundary = "MULTIPART_BYTERANGES";
    http_response response(status_codes::PartialContent);
    response.headers().add(U("Content-Type"), "multipart/byteranges; boundary=" + boundary);

    // Use a shared asynchronous stream buffer
    auto streambuf = std::make_shared<concurrency::streams::producer_consumer_buffer<uint8_t>>();
    auto ostream = streambuf->create_ostream();
    response.set_body(streambuf->create_istream());

    // Reply to the request and asynchronously write chunks
    request.reply(response);

    pplx::create_task([ranges, filepath, ostream, boundary] {
      try {
	      
	for (const auto& range : ranges) {
	  size_t start = range.first;
	  size_t end = range.second;

	  // Read the byte range
	  auto byte_data = read_byte_range(filepath, start, end);

	  // Write the part headers
	  std::ostringstream part_headers;
	  part_headers << "--" << boundary << "\r\n";
	  part_headers << "Content-Type: application/octet-stream\r\n";
	  part_headers << "Content-Range: bytes " << start << "-" << (end - 1) << "\r\n\r\n";
	  ostream.print(part_headers.str()).wait();

	  // Write the byte range data
	  ostream.streambuf().putn_nocopy(byte_data.data(), byte_data.size()).wait();
	  ostream.print("\r\n").wait();
	}

	// Write the closing boundary
	std::string closing_boundary = "--" + boundary + "--\r\n";
	ostream.print(closing_boundary).wait();

	// Close the stream
	ostream.close().wait();
		
      } catch (const std::exception& e) {
	ostream.close().wait();
      }
    });

  } catch (const std::exception& e) {
    request.reply(status_codes::InternalError, e.what());
  }
}

void execute_sql_on_parquet(const std::string &parquet_file,
                            const std::string &sql_query,
                            concurrency::streams::ostream out_stream) {
    duckdb::DuckDB db(nullptr); // In-memory database
    duckdb::Connection con(db);

    // Load the Parquet file into DuckDB
    std::ostringstream parquet_load_query;
    parquet_load_query << "CREATE TABLE parquet_data AS SELECT * FROM read_parquet('" << parquet_file << "');";
    con.Query(parquet_load_query.str());
    

    // Generate a unique temporary file name
    std::string temp_filename = generate_random_filename();

    // Prepare the COPY TO query
    std::ostringstream query_stream;
    query_stream << "COPY (" << sql_query << ") TO '" << temp_filename << "' (FORMAT CSV, HEADER TRUE);";
    // Execute the COPY TO command
    auto result = con.Query(query_stream.str());
    if (!result || result->HasError()) {
        std::remove(temp_filename.c_str()); // Clean up temporary file
        throw std::runtime_error("SQL query execution failed: " + result->GetError());
    }
    

    // Stream the temporary file to the output stream in chunks
    try {
        std::ifstream temp_file(temp_filename, std::ios::binary);
        if (!temp_file.is_open()) {
            throw std::runtime_error("Failed to open temporary file for streaming");
        }

        const size_t CHUNK_SIZE = 16 * 1024; // 16 KB chunks
        std::vector<uint8_t> buffer(CHUNK_SIZE);

        while (temp_file) {
            temp_file.read(reinterpret_cast<char *>(buffer.data()), buffer.size());
            std::streamsize bytes_read = temp_file.gcount();
            if (bytes_read > 0) {
                out_stream.streambuf().putn_nocopy(buffer.data(), bytes_read).wait();
            }
        }

        temp_file.close();
        std::remove(temp_filename.c_str()); // Delete the temporary file
	
        // Signal end of response
        out_stream.close().wait();
    } catch (const std::exception &e) {
        std::remove(temp_filename.c_str()); // Clean up the temporary file
        out_stream.close().wait();
        throw;
    }
}

std::string custom_url_decode(const std::string &value) {
  std::ostringstream decoded;
  for (size_t i = 0; i < value.length(); ++i) {
    if (value[i] == '+') {
      decoded << ' '; // Convert '+' to space
    } else if (value[i] == '%' && i + 2 < value.length()) {
      // Decode %XX to a character
      char hex[3] = {value[i + 1], value[i + 2], '\0'};
      decoded << static_cast<char>(std::strtol(hex, nullptr, 16));
      i += 2;
    } else {
      decoded << value[i];
    }
  }
  return decoded.str();
}

void handle_request_sql_query(http_request request) {
  try {
    // Parse query parameters
    auto query_params = uri::split_query(uri::decode(request.request_uri().query()));
    if (query_params.find(U("sql")) == query_params.end()) {
      request.reply(status_codes::BadRequest, "Missing 'sql' query parameter");
      return;
    }
    std::string sql_query = custom_url_decode(uri::decode(query_params[U("sql")]));
    std::string file_path = "/home/david/Documents/PhD/datasets/parquet_tpch/tpch_1000MB_lineitem_gzip.parquet"; // Path to your file

    // Check if the file exists
    if (!std::ifstream(file_path)) {
      request.reply(status_codes::NotFound, "File not found: " + file_path);
      return;
    }

    // Create a producer-consumer buffer for chunked streaming
    auto streambuf = concurrency::streams::producer_consumer_buffer<uint8_t>();
    auto out_stream = streambuf.create_ostream();

    // Set up the HTTP response for chunked transfer
    http_response response(status_codes::OK);
    response.headers().add(U("Content-Type"), U("text/csv"));
    response.set_body(streambuf.create_istream());
    request.reply(response);

    // Execute the SQL query and stream the output
    pplx::create_task([file_path, sql_query, out_stream]() {
      execute_sql_on_parquet(file_path, sql_query, out_stream);
    });
  } catch (const std::exception &e) {
    request.reply(status_codes::InternalError, e.what());
  }
}

// Main handler to route requests
void handle_request(http_request request) {
    auto path = uri::split_path(uri::decode(request.relative_uri().path()));

    if (path.empty()) {
        request.reply(status_codes::BadRequest, "Invalid request path");
        return;
    }

    // Route based on the path
    if (path[0] == "range") {
        handle_request_byte_ranges(request);
    } else if (path[0] == "query") {
        handle_request_sql_query(request);
    } else {
        request.reply(status_codes::NotFound, "Unknown path");
    }
}

int main() {
  try {
    // Set up the HTTP listener
    uri_builder uri(U("http://localhost:8080"));
    auto addr = uri.to_uri().to_string();
    http_listener listener(addr);

    listener.support(methods::GET, handle_request);

    std::cout << "Listening on " << addr << std::endl;
    listener.open().wait();

    std::string line;
    std::getline(std::cin, line); // Wait for user input to exit

    listener.close().wait();
  } catch (const std::exception& e) {
    std::cerr << "Error: " << e.what() << std::endl;
  }

  return 0;
}
