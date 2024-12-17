#include <iostream>
#include <string>
#include <curl/curl.h>

// Callback function to discard the response data from libcurl
static size_t WriteCallback(void* contents, size_t size, size_t nmemb, void* userp) {
  return size * nmemb; // Discard the response
}

// Function to perform a range request
void makeRangeRequest(const std::string& url, const std::string& range) {
  CURL* curl;
  CURLcode res;

  curl = curl_easy_init();

  if (curl) {
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);

    // Set the range header
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, ("Range: bytes=" + range).c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

    std::cout << "Making range request...\n";
    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
  }
}

// Function to perform a query request
void makeQueryRequest(const std::string& url, const std::string& query) {
  CURL* curl;
  CURLcode res;

  curl = curl_easy_init();

  if (curl) {
    std::string fullUrl = url + "?" + curl_easy_escape(curl, query.c_str(), query.length());
    curl_easy_setopt(curl, CURLOPT_URL, fullUrl.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, WriteCallback);

    std::cout << "Making query request...\n";
    res = curl_easy_perform(curl);

    if (res != CURLE_OK) {
      std::cerr << "curl_easy_perform() failed: " << curl_easy_strerror(res) << std::endl;
    }

    curl_easy_cleanup(curl);
  }
}

int main(int argc, char* argv[]) {
  curl_global_init(CURL_GLOBAL_DEFAULT);

  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <type> [args]\n";
    std::cerr << "Types:\n  range <url> <range>\n  query <url> <sql_query>\n";
    curl_global_cleanup();
    return 1;
  }

  std::string requestType = argv[1];

  if (requestType == "range" && argc == 4) {
    std::string url = argv[2];
    std::string range = argv[3];
    makeRangeRequest(url, range);
  } else if (requestType == "query" && argc == 4) {
    std::string url = argv[2];
    std::string query = argv[3];
    makeQueryRequest(url, "sql=" + query);
  } else {
    std::cerr << "Invalid arguments. Use 'range' or 'query' with correct parameters." << std::endl;
    curl_global_cleanup();
    return 1;
  }

  curl_global_cleanup();
  return 0;
}
