#include <cpprest/http_listener.h>
#include <cpprest/uri.h>
#include <cpprest/json.h>
#include <cpprest/producerconsumerstream.h>
#include <fstream>
#include <sstream>
#include <regex>
#include <vector>
#include <string>
#include <memory>

using namespace web;
using namespace web::http;
using namespace web::http::experimental::listener;

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
void handle_request(http_request request) {
  try {
    std::string filepath = "/home/david/Documents/PhD/datasets/parquet_tpch/tpch_1000MB_lineitem_uncompressed.parquet"; // Path to your file
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
