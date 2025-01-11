// #include <duckdb.hpp>
#include <fstream>
#include <sstream>
#include <regex>
#include <vector>
#include <string>
#include <memory>
#include <cstdio>
#include <iomanip>
#include <chrono>
#include <filesystem>
#include <random>
#include <sstream>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <arrow/compute/api.h>

#undef U

#include <cpprest/http_listener.h>
#include <cpprest/uri.h>
#include <cpprest/json.h>
#include <cpprest/producerconsumerstream.h>

using namespace web;
using namespace web::http;
using namespace web::http::experimental::listener;

void logNow(int64_t bytes = 0) {
    // Get the current time as a time_point
    auto now = std::chrono::system_clock::now();

    // Convert to a time_t for calendar time
    std::time_t current_time = std::chrono::system_clock::to_time_t(now);

    // Extract the fractional seconds (milliseconds)
    auto duration = now.time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration) % 1000;

    // Format the output
    // std::ofstream logFile("unfiltered_10_data_sent_log.txt");
    std::tm* local_time = std::localtime(&current_time); // Convert to local time
    std::cout << bytes << "SENT AT:" << std::put_time(local_time, "%Y-%m-%d %H:%M:%S") << "." 
              << std::setfill('0') << std::setw(3) << millis.count() << std::endl;
    // logFile.close();
}

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

std::shared_ptr<arrow::Table> readRowGroup(std::unique_ptr<parquet::arrow::FileReader>& reader, int row_group_index) {
  std::shared_ptr<arrow::Table> row_group_table;
  auto status = reader->ReadRowGroup(row_group_index, &row_group_table);
  if (!status.ok()) {
    throw std::runtime_error("Failed to read row group: " + status.ToString());
  }
  return row_group_table;
}

std::shared_ptr<arrow::Table> filterRowGroup(const std::shared_ptr<arrow::Table>& table, const std::string& column_name, int64_t threshold) {
  auto schema = table->schema();
  auto col_index = schema->GetFieldIndex(column_name);
  if (col_index == -1) {
    throw std::runtime_error("Column not found: " + column_name);
  }

  auto column = table->column(col_index);
  // auto chunked_array = column->chunked_array();

  // Assuming column is of type Int64 for simplicity
  // Filter the table using the mask
  // Filter the table using the mask
  auto filter_result = arrow::compute::CallFunction("greater", {table, arrow::Datum(threshold)});
  if (!filter_result.status().ok()) {
    throw std::runtime_error("Failed to filter table: " + filter_result.status().ToString());
  }

  auto mask = filter_result.ValueOrDie();

  // Filter the table using the mask
  auto filtered_result = arrow::compute::Filter(table, mask);
  if (!filtered_result.status().ok()) {
    throw std::runtime_error("Failed to filter table: " + filtered_result.status().ToString());
  }

  return filtered_result.ValueOrDie().table();
}

std::shared_ptr<arrow::Table> filterRowGroupByProportion(const std::shared_ptr<arrow::Table>& table, const std::vector<std::string>& column_names, double proportion) {
  if (proportion <= 0.0 || proportion > 1.0) {
    throw std::invalid_argument("Proportion must be between 0 and 1.");
  }

  auto schema = table->schema();
  int64_t num_rows = table->num_rows();

  // Generate a single mask
  std::vector<bool> mask_values(num_rows, false);
  int64_t num_selected = static_cast<int64_t>(num_rows * proportion);

  std::vector<int64_t> indices(num_rows);
  std::iota(indices.begin(), indices.end(), 0);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::shuffle(indices.begin(), indices.end(), gen);

  for (int64_t i = 0; i < num_selected; ++i) {
    mask_values[indices[i]] = true;
  }

  // Convert mask to an Arrow BooleanArray
  arrow::BooleanBuilder mask_builder;
  for (bool value : mask_values) {
    auto append_status = mask_builder.Append(value);
    if (!append_status.ok()) {
      throw std::runtime_error("Failed to append to mask builder: " + append_status.ToString());
    }
  }
  std::shared_ptr<arrow::Array> mask;
  auto mask_status = mask_builder.Finish(&mask);
  if (!mask_status.ok()) {
    throw std::runtime_error("Failed to create mask: " + mask_status.ToString());
  }

  // Filter the table using the mask
  arrow::compute::ExecContext exec_context;
  auto filter_result = arrow::compute::Filter(table, mask);
  if (!filter_result.status().ok()) {
    throw std::runtime_error("Failed to filter table: " + filter_result.status().ToString());
  }

  auto filtered_table = filter_result.ValueOrDie().table();

  // Select only the specified columns
  std::vector<std::shared_ptr<arrow::Field>> selected_fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> selected_columns;
  for (const auto& column_name : column_names) {
    auto col_index = schema->GetFieldIndex(column_name);
    if (col_index == -1) {
      throw std::runtime_error("Column not found: " + column_name);
    }
    selected_fields.push_back(schema->field(col_index));
    selected_columns.push_back(filtered_table->column(col_index));
  }

  auto selected_schema = std::make_shared<arrow::Schema>(selected_fields);
  return arrow::Table::Make(selected_schema, selected_columns);
}

void writeFilteredRowGroup(std::unique_ptr<parquet::arrow::FileWriter>& writer, const std::shared_ptr<arrow::Table>& table) {
  auto status = writer->WriteTable(*table, 1024);
  if (!status.ok()) {
    throw std::runtime_error("Failed to write row group to output Parquet file: " + status.ToString());
  }
}

std::shared_ptr<arrow::Buffer> writeRowGroupToBuffer(const std::shared_ptr<arrow::Table>& table, const std::shared_ptr<parquet::WriterProperties>& writer_properties) {
    // Create a BufferOutputStream to hold the serialized data
    std::shared_ptr<arrow::io::BufferOutputStream> buffer_output_stream;
    auto buffer_result = arrow::io::BufferOutputStream::Create();
    if (!buffer_result.ok()) {
        throw std::runtime_error("Failed to create BufferOutputStream: " + buffer_result.status().ToString());
    }
    buffer_output_stream = buffer_result.ValueOrDie();

    // Create a Parquet writer using the buffer
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    auto writer_status = parquet::arrow::FileWriter::Open(*table->schema(), arrow::default_memory_pool(), buffer_output_stream, writer_properties, &writer);
    if (!writer_status.ok()) {
        throw std::runtime_error("Failed to create Parquet writer: " + writer_status.ToString());
    }

    // Write the table to the buffer
    auto write_status = writer->WriteTable(*table, 1024); // Use a suitable row group size
    if (!write_status.ok()) {
        throw std::runtime_error("Failed to write row group to buffer: " + write_status.ToString());
    }

    // Finalize the writer
    auto close_status = writer->Close();
    if (!close_status.ok()) {
        throw std::runtime_error("Failed to finalize Parquet writer: " + close_status.ToString());
    }

    // Return the serialized data as a buffer
    return buffer_output_stream->Finish().ValueOrDie();
}

void processParquetFileInBatches(const std::string& input_file, const std::vector<std::string> &column_names, const double proportion, concurrency::streams::ostream out_stream) {
  // Open input file
  std::shared_ptr<arrow::io::ReadableFile> infile;
  auto infile_result = arrow::io::ReadableFile::Open(input_file);
  if (!infile_result.ok()) {
    throw std::runtime_error("Failed to open input Parquet file: " + infile_result.status().ToString());
  }
  infile = infile_result.ValueOrDie();

  // Create Parquet file reader
  std::unique_ptr<parquet::arrow::FileReader> reader;
  auto reader_status = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
  if (!reader_status.ok()) {
    throw std::runtime_error("Failed to create Parquet reader: " + reader_status.ToString());
  }

  // Get writer properties
  auto writer_properties = parquet::WriterProperties::Builder().compression(parquet::Compression::GZIP)->build();

  // // Open output file
  // std::shared_ptr<arrow::io::FileOutputStream> outfile;
  // auto outfile_result = arrow::io::FileOutputStream::Open(output_file);
  // if (!outfile_result.ok()) {
  //   throw std::runtime_error("Failed to open output Parquet file: " + outfile_result.status().ToString());
  // }
  // outfile = outfile_result.ValueOrDie();

  // // Prepare Parquet writer
  // std::unique_ptr<parquet::arrow::FileWriter> writer;
  // auto writer_status = parquet::arrow::FileWriter::Open(*reader->schema(), arrow::default_memory_pool(), outfile, writer_properties);
  // if (!writer_status.ok()) {
  //   throw std::runtime_error("Failed to create Parquet writer: " + writer_status.status().ToString());
  // }
  // writer = std::move(writer_status).ValueOrDie();

  // Process each row group
  int num_row_groups = reader->num_row_groups();
  for (int i = 0; i < num_row_groups; ++i) {
    auto row_group_table = readRowGroup(reader, i);
    auto filtered_table = filterRowGroupByProportion(row_group_table, column_names, proportion);
    auto buffer = writeRowGroupToBuffer(filtered_table, writer_properties);
    out_stream.streambuf().putn_nocopy(buffer->data(), buffer->size()).wait();
    // writeFilteredRowGroup(writer, filtered_table);
  }

  // // Finalize writing
  // auto close_status = writer->Close();
  // if (!close_status.ok()) {
  //   throw std::runtime_error("Failed to finalize Parquet writer: " + close_status.ToString());
  // }
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
    std::string filepath = "/home/ubuntu/tpch_1000MB_lineitem_gzip.parquet"; // Path to your file
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
	  // logNow(byte_data.size());
	  ostream.print("\r\n").wait();
	}

	// Write the closing boundary
	std::string closing_boundary = "--" + boundary + "--\r\n";
	ostream.print(closing_boundary).wait();

	// Close the stream
	logNow();
	ostream.close().wait();
		
      } catch (const std::exception& e) {
	ostream.close().wait();
      }
    });

  } catch (const std::exception& e) {
    request.reply(status_codes::InternalError, e.what());
  }
}

// void execute_sql_on_parquet(const std::string &parquet_file,
//                             const std::string &sql_query,
//                             concurrency::streams::ostream out_stream) {

//     duckdb::DBConfig config;
//     config.options.maximum_threads = 1;
//     duckdb::DuckDB db(nullptr, &config); // In-memory database
//     duckdb::Connection con(db);

//     // Load the Parquet file into DuckDB
//     std::ostringstream parquet_load_query;
//     parquet_load_query << "CREATE TABLE parquet_data AS SELECT * FROM read_parquet('" << parquet_file << "');";
//     con.Query(parquet_load_query.str());
    

//     // Generate a unique temporary file name
//     std::string temp_filename = generate_random_filename();

//     // Prepare the COPY TO query
//     std::ostringstream query_stream;
//     query_stream << "COPY (" << sql_query << ") TO '" << temp_filename << "' (FORMAT CSV, HEADER TRUE);";
//     // Execute the COPY TO command
//     auto result = con.Query(query_stream.str());
//     if (!result || result->HasError()) {
//         std::remove(temp_filename.c_str()); // Clean up temporary file
//         throw std::runtime_error("SQL query execution failed: " + result->GetError());
//     }
    

//     // Stream the temporary file to the output stream in chunks
//     try {
//         std::ifstream temp_file(temp_filename, std::ios::binary);
//         if (!temp_file.is_open()) {
//             throw std::runtime_error("Failed to open temporary file for streaming");
//         }

//         const size_t CHUNK_SIZE = 16 * 1024; // 16 KB chunks
//         std::vector<uint8_t> buffer(CHUNK_SIZE);
//         while (temp_file) {
//             temp_file.read(reinterpret_cast<char *>(buffer.data()), buffer.size());
//             std::streamsize bytes_read = temp_file.gcount();
//             if (bytes_read > 0) {
//                 out_stream.streambuf().putn_nocopy(buffer.data(), bytes_read).wait();
// 		// logNow(bytes_read);
// 	    }
//         }

// 	// auto size = std::filesystem::file_size(temp_filename);
// 	// std::cout << "FILE SIZE: " << size << std::endl;
//         temp_file.close();
//         std::remove(temp_filename.c_str()); // Delete the temporary file
	
//         // Signal end of response
// 	logNow();
//         out_stream.close().wait();
//     } catch (const std::exception &e) {
//         std::remove(temp_filename.c_str()); // Clean up the temporary file
//         out_stream.close().wait();
//         throw;
//     }
// }

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

std::vector<std::string> parse_column_name_list(const std::string &column_names) {
  std::vector<std::string> res;
  std::string column_name;
  std::stringstream col_stream;
  col_stream << column_names;

  while (std::getline(col_stream, column_name, '-')) {
    res.push_back(column_name);
  }

  return std::move(res);
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
    std::string file_path = "/home/ubuntu/tpch_1000MB_lineitem_gzip.parquet"; // Path to your file

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
      // execute_sql_on_parquet(file_path, sql_query, out_stream);
    });
    
  } catch (const std::exception &e) {
    request.reply(status_codes::InternalError, e.what());
  }
}

void handle_request_selectivity_query(http_request request) {
  try {
    // Parse query parameters
    auto query_params = uri::split_query(uri::decode(request.request_uri().query()));
    if (query_params.find(U("selectivity")) == query_params.end()) {
      request.reply(status_codes::BadRequest, "Missing 'selectivity' query parameter");
      return;
    }
    if (query_params.find(U("columns")) == query_params.end()) {
      request.reply(status_codes::BadRequest, "Missing 'selectivity' query parameter");
      return;
    }
    double selectivity = std::stod(custom_url_decode(uri::decode(query_params[U("selectivity")])));
    std::vector<std::string> column_names = parse_column_name_list(uri::decode(query_params[U("columns")]));
    std::string file_path = "/home/ubuntu/tpch_1000MB_lineitem_gzip.parquet"; // Path to your file

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
    response.headers().add(U("Content-Type"), U("application/vnd.apache.parquet"));
    response.set_body(streambuf.create_istream());
    request.reply(response);

    // Execute the SQL query and stream the output
    pplx::create_task([file_path, selectivity, column_names, out_stream]() {
      processParquetFileInBatches(file_path, column_names, selectivity, out_stream);
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
    } else if (path[0] == "select") {
        handle_request_selectivity_query(request);
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
