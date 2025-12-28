#include "query5.hpp"
// #include"tables_soa.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <map>
#include <chrono>
using Clock = std::chrono::high_resolution_clock;
using ms = std::chrono::milliseconds;
// TODO: Include additional headers as needed

int main(int argc, char* argv[]) {
    std::string r_name, start_date, end_date, table_path, result_path;
    int num_threads;

    if (!parseArgs(argc, argv, g_config.r_name, g_config.start_date, g_config.end_date, g_config.num_threads, g_config.table_path, g_config.result_path)) {
        std::cerr << "Failed to parse command line arguments." << std::endl;
        return 1;
    }
    std::cout << "Arguments parsed successfully." << std::endl;
    std::cout << "Region Name: " << g_config.r_name << std::endl;
    std::cout << "Start Date: " << g_config.start_date << std::endl; 
    std::cout << "End Date: " << g_config.end_date << std::endl;
    std::cout << "Number of Threads: " << g_config.num_threads << std::endl;
    std::cout << "Table Path: " << g_config.table_path << std::endl;
    std::cout << "Result Path: " << g_config.result_path << std::endl;

    auto t0 = Clock::now();

    // tables customer_data = CustomerSOA(), orders_data = OrdersSOA(), lineitem_data = LineItemSOA(), supplier_data = SupplierSOA(), nation_data = NationSOA(), region_data = RegionSOA();
    CustomerSOA customer_data;
    OrdersSOA orders_data;
    LineItemSOA lineitem_data;
    SupplierSOA supplier_data;
    NationSOA nation_data;
    RegionSOA region_data;
    if (!readTPCHData(g_config.table_path, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data)) {
        std::cout << "Failed to read TPCH data." << std::endl;
        return 1;
    }

    
    std::map<std::string, double> results;
    auto t1 = Clock::now();
    auto load_duration = std::chrono::duration_cast<ms>(t1 - t0).count();
    std::cout << "Data loading completed in " << load_duration << " ms." << std::endl;


    auto t2 = Clock::now();
    if (!executeQuery5(g_config.r_name, g_config.start_date, g_config.end_date, g_config.num_threads, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data, results)) {
        std::cout << "Failed to execute TPCH Query 5." << std::endl;
        return 1;
    }
    auto t3 = Clock::now();
    auto query_duration = std::chrono::duration_cast<ms>(t3 - t2).count();
    std::cout << "Query execution completed in " << query_duration << " ms." << std::endl;
    auto total_duration = std::chrono::duration_cast<ms>(t3 - t0).count();
    std::cout << "Total execution time: " << total_duration << " ms." << std::endl;
    if (!outputResults(g_config.result_path, results)) {
        std::cerr << "Failed to output results." << std::endl;
        return 1;
    }

    std::cout << "TPCH Query 5 implementation completed." << std::endl;

    return 0;
} 