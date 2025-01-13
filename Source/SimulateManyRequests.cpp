#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <thread>
#include <string>
#include <algorithm>
#include <random>
#include <cmath>
#include <fstream>
#include <cstdlib> // For system()

template<typename T>
T randomBetween(T min, T max) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<T> dist(min, max);
  return dist(gen);
}

std::vector<std::pair<int64_t, int64_t>> generateRanges(int64_t start, int64_t end, double_t proportion, int64_t numRanges) {
  // Validate inputs
  if (start >= end || proportion < 0 || proportion > 1 || numRanges <= 0) {
    throw std::invalid_argument("Invalid input parameters.");
  }

  // Calculate total range size and target coverage size
  auto rangeSize = end - start;
  auto avgBlockSize = static_cast<int64_t>(std::round(rangeSize / numRanges));
  auto targetBlockCoverage = static_cast<int64_t>(std::round(avgBlockSize * proportion));
  
  std::vector<std::pair<int64_t, int64_t>> ranges;
  ranges.reserve(numRanges);
  
  for (int64_t rangesDone = 0, currBlockStart = start; rangesDone < numRanges && currBlockStart < end; rangesDone++, currBlockStart += avgBlockSize) {
    int64_t randRangeStartInBlock = randomBetween(currBlockStart, currBlockStart + targetBlockCoverage);
    
    ranges.emplace_back(randRangeStartInBlock, randRangeStartInBlock + targetBlockCoverage);
  }

  return std::move(ranges);
}

std::vector<std::string> convertRangePairsToStringRanges(const std::vector<std::pair<int64_t, int64_t>>& ranges, int64_t rangesPerString) {

  auto numRanges = ranges.size();
  int64_t numStrings = (numRanges / rangesPerString) + 1;
  std::vector<std::string> res;
  res.reserve(numStrings);

  auto rangesDone = 0;
  int64_t currRangesInString = 0;
  std::stringstream currString;
  while (rangesDone < numRanges) {
    currRangesInString = std::min(rangesPerString, static_cast<int64_t>(numRanges - rangesDone));

    for (size_t i = 0; i < currRangesInString - 1; i++) {
      auto& range = ranges[i + rangesDone];
      auto& start = range.first;
      auto& end = range.second;
      currString << start << "-" << end << ",";
    }
    auto& range = ranges[currRangesInString + rangesDone - 1];
    auto& start = range.first;
    auto& end = range.second;
    currString << start << "-" << end;

    res.push_back(currString.str());
    currString.str(std::string());
    rangesDone += currRangesInString;
  }

  return std::move(res);
}

// Function to execute the request program with arguments
void callRequestProgram(const std::string& executablePath, const std::string& requestType, const std::string& url, const std::string& param, int id) {
  std::string command = executablePath + " " + requestType + " " + url + " " + param;
  // std::cout << "Thread " << id << " executing: " << command << std::endl;
  int result = system(command.c_str());
  if (result != 0) {
    std::cerr << "Thread " << id << " failed to execute command." << std::endl;
  }
  
  //  else {
  //   std::cout << "Thread " << id << " completed successfully." << std::endl;
  // }
}

// Function to execute the request program with arguments
void callRequestProgramSelect(const std::string& executablePath, const std::string& requestType, const std::string& url, const std::string& selectivityParam, const std::string& columnsParam, int id) {
  std::string command = executablePath + " " + requestType + " " + url + " " + selectivityParam + " " + columnsParam;
  // std::cout << "Thread " << id << " executing: " << command << std::endl;
  int result = system(command.c_str());
  // if (result != 0) {
  //   std::cerr << "Thread " << id << " failed to execute command." << std::endl;
  // } else {
  //   std::cout << "Thread " << id << " completed successfully." << std::endl;
  // }
}

// Function to execute the request program with arguments
void callRequestProgramRanges(const std::string& executablePath, const std::string& requestType, const std::string& url, const std::vector<std::string>& params, const std::string& selectivityParam, int id) {
  // std::cout << "Thread " << id << " executing: " << std::endl;
  int result;
  for (const std::string& param : params) {
    std::string command = executablePath + " " + requestType + " " + url + " " + param + " " + selectivityParam;
    // std::cout << "   " << command << std::endl;
    result = system(command.c_str());
    if (result != 0) {
      break;
    }
  }
  
  if (result != 0) {
    std::cerr << "Thread " << id << " failed to execute command." << std::endl;
  }
  //  else {
  //   std::cout << "Thread " << id << " completed successfully." << std::endl;
  // }
}

int main(int argc, char* argv[]) {
  if (argc != 4 && argc != 5) {
    std::cerr << "Usage: " << argv[0] << " <path_to_executable> <request_type> <n> <selectivity {0-1}>?\n";
    std::cerr << "Request types:\n  1. range\n  2. unfiltered_query\n  3. filtered_query\n  4. select\n";
    return 1;
  }

  std::string executablePath = argv[1];
  std::string requestType = argv[2];
  int n = std::stoi(argv[3]);

  if (n <= 0) {
    std::cerr << "Number of concurrent requests (n) must be greater than 0." << std::endl;
    return 1;
  }

  std::string url;
  std::string param;
  std::vector<std::thread> threads;

  // Determine request type and parameter
  if (requestType == "range" && argc == 5) {
    url = "http://localhost:8080/range";
    std::string selectivityParam = argv[4];
    double selectivity = std::stod(selectivityParam);
    const auto ranges = generateRanges(0, 1717986918, selectivity, 30000);
    // std::cout << "Generated Ranges: " << ranges.size() << std::endl;
    std::vector<std::string> params = convertRangePairsToStringRanges(ranges, 255);
    // std::cout << "Generated Range Strings: " << params.size() << std::endl;
    // Launch n threads for query requests
    for (int i = 0; i < n; ++i) {
      threads.emplace_back(callRequestProgramRanges, executablePath, requestType, url, params, selectivityParam, i + 1);
    }
    // Wait for all threads to complete
    for (auto& t : threads) {
      t.join();
    }
    return 0;
  } else if (requestType == "unfiltered_query") {
    requestType = "query";
    url = "http://localhost:8080/query";
    param = "\"SELECT l_shipdate, l_discount, l_quantity, l_extendedprice FROM parquet_data\"";
  } else if (requestType == "filtered_query") {
    requestType = "query";
    url = "http://localhost:8080/query";
    param = "\"SELECT l_discount, l_extendedprice FROM parquet_data WHERE l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01' AND l_discount >= 0.059 AND l_discount <= 0.061 AND l_quantity < 24\"";
  } else if (requestType == "select" && argc == 5) {
    requestType = "select";
    url = "http://localhost:8080/select";
    std::string selectivityParam = argv[4];
    std::string columnsParam = "l_quantity-l_orderkey-l_partkey-l_suppkey-l_linenumber-l_extendedprice-l_discount-l_tax-l_returnflag-l_linestatus-l_shipdate-l_commitdate-l_receiptdate-l_shipinstruct-l_shipmode-l_comment";
    // std::string columnsParam = "l_quantity";

    for (int i = 0; i < n; ++i) {
      threads.emplace_back(callRequestProgramSelect, executablePath, requestType, url, selectivityParam, columnsParam, i + 1);
    }
    for (auto& t : threads) {
      t.join();
    }
    return 0;
  } else {
    std::cerr << "Invalid request type. Use 'range', 'unfiltered_query', 'filtered_query', or 'select'." << std::endl;
    return 1;
  }

  // Launch n threads for query requests
  for (int i = 0; i < n; ++i) {
    threads.emplace_back(callRequestProgram, executablePath, requestType, url, param, i + 1);
  }

  // Wait for all threads to complete
  for (auto& t : threads) {
    t.join();
  }

  std::cout << "All requests completed." << std::endl;
  return 0;
}
