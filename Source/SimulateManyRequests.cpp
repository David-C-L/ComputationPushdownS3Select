#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <thread>
#include <string>
#include <fstream>
#include <cstdlib> // For system()

inline std::vector<std::string> getBounds() {
  return {
    "0-749,1544551699-1544551990",
    "1544473056-1544551699",
    "1268028-2220734,2933017-3092550",
    "6850197-7802903,8515935-8674719",
    "12431617-13383575,14096606-14256140",
    "18013787-18965744,19678776-19838309",
    "23594458-24547164,25260196-25418980",
    "29175129-30127086,30840118-30999651",
    "34755801-35707758,36420789-36580323",
    "40336472-41289178,42001461-42160994",
    "45917892-46869849,47582881-47742414",
    "51499312-52452018,53165050-53323835",
    "57079984-58031941,58744972-58903757",
    "62659906-63611863,64324895-64484428",
    "68243573-69196279,69909311-70068095",
    "73826491-74778448,75491480-75651013",
    "79407163-80359869,81072900-81231685",
    "84990081-85942038,86655070-86814603",
    "90570003-91521960,92234992-92394525",
    "96149925-97102631,97814914-97974448",
    "101730597-102682554,103395585-103555119",
    "107312766-108264723,108977755-109136539",
    "112893437-113845394,114558426-114717959",
    "118474857-119426815,120139846-120298631",
    "124052533-125005239,125717522-125877055",
    "129633953-130585910,131298942-131458475",
    "135216122-136168079,136881111-137040644",
    "140797542-141749500,142462531-142622065"
  };
}

std::vector<std::string> readStringsFromFile(const std::string& filePath) {
    std::vector<std::string> lines;
    std::ifstream file(filePath);

    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << filePath << std::endl;
        return lines; // Return an empty vector on failure
    }

    std::string line;
    while (std::getline(file, line)) {
        lines.push_back(line);
    }

    file.close();
    return lines;
}

void logNow(int id) {
    // Get the current time as a time_point
    auto now = std::chrono::system_clock::now();

    // Convert to a time_t for calendar time
    std::time_t current_time = std::chrono::system_clock::to_time_t(now);

    // Extract the fractional seconds (milliseconds)
    auto duration = now.time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration) % 1000;

    // Format the output
    std::tm* local_time = std::localtime(&current_time); // Convert to local time
    std::cout << "THREAD " << id << " DONE AT:" << std::put_time(local_time, "%Y-%m-%d %H:%M:%S") << "." 
              << std::setfill('0') << std::setw(3) << millis.count() << std::endl;
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
void callRequestProgramRanges(const std::string& executablePath, const std::string& requestType, const std::string& url, const std::vector<std::string>& params, int id) {
  // std::cout << "Thread " << id << " executing: " << std::endl;
  int result;
  for (const std::string& param : params) {
    std::string command = executablePath + " " + requestType + " " + url + " " + param;
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
  if (requestType == "range") {
    url = "http://localhost:8080/range";
    std::vector<std::string> params = getBounds();
    // Launch n threads for query requests
    for (int i = 0; i < n; ++i) {
      threads.emplace_back(callRequestProgramRanges, executablePath, requestType, url, params, i + 1);
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
    std::string columnsParam = "l_quantity";

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
