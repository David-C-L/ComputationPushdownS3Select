#define CROW_MAIN
#include "ParquetServer.hpp"
#include "crow.h"
#include <fstream>
#include <vector>
#include <regex>
#include <sstream>

// Function to read a single byte range from a file
std::vector<char> read_byte_range(const std::string& filepath, size_t start, size_t end) {
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
    std::vector<char> buffer(range_size);
    file.read(buffer.data(), range_size);
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

int main() {
    crow::SimpleApp app;

    // Endpoint to serve byte streams using crow::multipart
    CROW_ROUTE(app, "/file")([](const crow::request& req, crow::response& res) {
        try {
            std::string filepath = "example.parquet"; // Path to your file

            // Parse the Range header
            std::string range_header = req.get_header_value("Range");
            if (range_header.empty() || range_header.rfind("bytes=", 0) != 0) {
                res.code = 416; // Range Not Satisfiable
                res.end("Invalid or missing Range header");
                return;
            }

            // Extract byte ranges
            std::vector<std::pair<size_t, size_t>> ranges = parse_ranges(range_header);
            if (ranges.empty()) {
                res.code = 416; // Range Not Satisfiable
                res.end("No valid ranges provided");
                return;
            }

            // Use crow::multipart to construct the multipart/byteranges response
            crow::multipart::message multipart_response;
            for (const auto& [start, end] : ranges) {
                // Read the byte range
                std::vector<char> byte_data = read_byte_range(filepath, start, end);

                // Create a part for this byte range
                crow::multipart::part part;
                part.headers["Content-Type"] = "application/octet-stream";
                part.headers["Content-Range"] = "bytes " + std::to_string(start) + "-" + std::to_string(end - 1);
                part.body = std::string(byte_data.begin(), byte_data.end());

                // Add the part to the multipart message
                multipart_response.parts.push_back(std::move(part));
            }

            // Set the response as a multipart/byteranges response
            res.code = 206; // Partial Content
            res.set_header("Content-Type", "multipart/byteranges; boundary=" + multipart_response.boundary);
            res.body = multipart_response.dump(); // Serialize the multipart message
            res.end();
        } catch (const std::exception& e) {
            res.code = 500; // Internal Server Error
            res.end(e.what());
        }
    });

    app.port(8080).multithreaded().run();
    return 0;
}
