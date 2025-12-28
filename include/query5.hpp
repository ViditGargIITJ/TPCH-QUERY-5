#ifndef QUERY5_HPP
#define QUERY5_HPP

#include <string>
#include <vector>
#include <map>
#include "tables_soa.hpp"

#pragma once
#include <string>

struct Config {
    int num_threads = 1;
    std::string r_name;
    std::string start_date;
    std::string end_date;
    std::string table_path;
    std::string result_path;
};

// Global configuration object
extern Config g_config;

class tables; // Forward declaration

// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path);

// Function to read TPCH data from the specified paths
// bool readTPCHData(const std::string& table_path, tables& customer_data, tables& orders_data, tables& lineitem_data, tables& supplier_data, tables& nation_data, tables& region_data);
bool readTPCHData(const std::string& table_path, CustomerSOA& customer_data, OrdersSOA& orders_data,
                  LineItemSOA& lineitem_data, SupplierSOA& supplier_data, NationSOA& nation_data, RegionSOA& region_data);
//  Function to execute TPCH Query 5 using multithreading
// bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const tables& customer_data, const tables& orders_data, const tables& lineitem_data, const tables& supplier_data, const tables& nation_data, const tables& region_data, std::map<std::string, double>& results);
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads,
                   const CustomerSOA& customer_data, const OrdersSOA& orders_data, const LineItemSOA& lineitem_data,
                   const SupplierSOA& supplier_data, const NationSOA& nation_data, const RegionSOA& region_data,
                   std::map<std::string, double>& results);

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results);

#endif // QUERY5_HPP 

