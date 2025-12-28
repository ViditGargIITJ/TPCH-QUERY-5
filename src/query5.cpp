#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <future> 
#include <unordered_map>
#include <iomanip> 
#include "tables_soa.hpp"

Config g_config;


// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    if (argc != 13) { // Expecting 6 key-value pairs + program name
        std::cerr << "Usage: " << argv[0] << " --r_name <region_name> --start_date <date> --end_date <date> --threads <num_threads> --table_path <path> --result_path <path>" << std::endl;
        return false;
    }

    for (int i = 1; i < argc; i += 2) {
        std::string arg = argv[i];
        if (i + 1 >= argc) {
            std::cerr << "Missing value for argument: " << arg << std::endl;
            return false;
        }

        if (arg == "--r_name") {
            r_name = argv[i + 1];
        } else if (arg == "--start_date") {
            start_date = argv[i + 1];
        } else if (arg == "--end_date") {
            end_date = argv[i + 1];
        } else if (arg == "--threads") {
            try {
                num_threads = std::stoi(argv[i + 1]);
            } catch (const std::invalid_argument&) {
                std::cerr << "Invalid number for --threads: " << argv[i + 1] << std::endl;
                return false;
            }
        } else if (arg == "--table_path") {
            table_path = argv[i + 1];
        } else if (arg == "--result_path") {
            result_path = argv[i + 1];
        } else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            return false;
        }
    }
    return true;
}


// // Function to read TPCH data from the specified path

void readChunk(const std::string &file_path,
               long start,
               long end,
               tables* out)
{
    std::ifstream file(file_path, std::ios::binary);
    file.seekg(start);

    std::string line;

    if (start != 0)
        std::getline(file, line);   // skip partial line

    long pos = file.tellg();

    while (std::getline(file, line)) {
        try {
            out->insert_line(line);
        } catch (const std::exception& e) {
            std::cerr << "Parse error in " << file_path
                    << " line: " << line
                    << " reason: " << e.what() << std::endl;
            throw;
        }

        pos = file.tellg();
        if (pos == -1 || pos >= end)
            break;
    }
}


void load_data_multithreaded(
    const std::string& file_path,
    tables& output,
    int num_threads)
{
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Failed to open file: " << file_path << std::endl;
        return;
    }

    file.seekg(0, std::ios::end);
    long fileSize = file.tellg();
    file.close();

    long chunkSize = fileSize / num_threads;

    std::vector<std::unique_ptr<tables>> thread_data;
    thread_data.reserve(num_threads);

    for (int i = 0; i < num_threads; i++)
        thread_data.push_back(output.create_empty());

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        long start = t * chunkSize;
        long end   = (t == num_threads - 1) ? fileSize : start + chunkSize;

        threads.emplace_back(
            readChunk,
            std::ref(file_path),
            start,
            end,
            thread_data[t].get()
        );
    }

    for (auto& th : threads)
        th.join();

    // std::cout << "All threads completed loading data.\n";

    // -------- Merge ----------
    for (int t = 0; t < num_threads; ++t)
        output.merge_from(*thread_data[t]);

    // std::cout << "Merged table successfully.\n";
}


// Updated readTPCHData to use multithreaded loading

bool readTPCHData(const std::string& table_path,
                  CustomerSOA& customer_data,
                  OrdersSOA& orders_data,
                  LineItemSOA& lineitem_data,
                  SupplierSOA& supplier_data,
                  NationSOA& nation_data,
                  RegionSOA& region_data)  
{
try {
        int num_threads = g_config.num_threads;
        // int num_threads = 4;
        std::cout <<"Using "<<num_threads<<" threads to load data."<<std::endl;

        load_data_multithreaded(table_path + "\\" +"customer.tbl", customer_data, num_threads);
        std::cout << "Loaded " << customer_data.size() << " customer records." << std::endl;

        load_data_multithreaded(table_path + "\\" + "orders.tbl", orders_data, num_threads);
        std::cout << "Loaded " << orders_data.size() << " orders records." << std::endl;

        load_data_multithreaded(table_path + "\\" + "lineitem.tbl", lineitem_data, num_threads);
        std::cout << "Loaded " << lineitem_data.size() << " lineitem records." << std::endl;

        load_data_multithreaded(table_path + "\\" + "supplier.tbl", supplier_data, num_threads);
        std::cout << "Loaded " << supplier_data.size() << " supplier records." << std::endl;

        load_data_multithreaded(table_path + "\\" + "nation.tbl", nation_data, num_threads);
        std::cout << "Loaded " << nation_data.size() << " nation records." << std::endl;

        load_data_multithreaded(table_path + "\\" + "region.tbl", region_data, num_threads);
        std::cout << "Loaded " << region_data.size() << " region records." << std::endl;
    
        return true;
    } catch (const std::exception& e) {
        std::cout << "Error loading TPCH data: " << e.what() << std::endl;
        return false;
    }
}

// QUERY 5 WORKER FUNCTIONS

void lineitem_worker(
    size_t start,
    size_t end,
    const LineItemSOA& lineitem,
    const std::unordered_map<int,int>& order_to_nation,
    const std::unordered_map<int,int>& supp_to_nation,
    const std::unordered_map<int,std::string>& nationkey_to_name,
    std::unordered_map<std::string,double>& local_result
){
    for(size_t i=start;i<end;++i){
        int order = lineitem.l_orderkey[i];
        int supp  = lineitem.l_suppkey[i];

        auto oit = order_to_nation.find(order);
        if(oit == order_to_nation.end()) continue;

        auto sit = supp_to_nation.find(supp);
        if(sit == supp_to_nation.end()) continue;

        if(oit->second != sit->second) continue;

        double price = lineitem.l_extendedprice[i];
        double disc  = lineitem.l_discount[i];
        double revenue = price*(1.0 - disc);

        const std::string& name = nationkey_to_name.at(oit->second);
        local_result[name] += revenue;
    }
}


void orders_worker(
    size_t start,
    size_t end,
    const OrdersSOA& orders,
    const std::unordered_map<int,int>& cust_to_nation,
    const std::string& start_date,
    const std::string& end_date,
    std::unordered_map<int,int>& local_map
){
    for(size_t i=start;i<end;++i){

        const std::string& date = orders.o_orderdate[i];
        if(date < start_date || date >= end_date)
            continue;

        int cust = orders.o_custkey[i];
        auto it = cust_to_nation.find(cust);
        if(it == cust_to_nation.end()) continue;

        local_map[orders.o_orderkey[i]] = it->second;
    }
}

void customer_worker(
    size_t start,
    size_t end,
    const CustomerSOA& customer,
    std::unordered_map<int,int>& local_map
){
    for(size_t i=start;i<end;++i){
        local_map[customer.c_custkey[i]] = customer.c_nationkey[i];
    }
}


void supplier_worker(
    size_t start,
    size_t end,
    const SupplierSOA& supplier,
    const std::unordered_map<int,std::string>& nationkey_to_name,
    std::unordered_map<int,int>& local_map
){
    for (size_t i = start; i < end; ++i) {
        int nationkey = supplier.s_nationkey[i];

        auto it = nationkey_to_name.find(nationkey);
        if (it == nationkey_to_name.end()) continue;

        local_map[supplier.s_suppkey[i]] = nationkey;
    }
}


// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name,
                   const std::string& start_date,
                   const std::string& end_date,
                   int num_threads,
                   const CustomerSOA& customer_data,
                   const OrdersSOA& orders_data,
                   const LineItemSOA& lineitem_data,
                   const SupplierSOA& supplier_data,
                   const NationSOA& nation_data,
                   const RegionSOA& region_data,
                   std::map<std::string, double>& results)
    // TODO: Implement TPCH Query 5 using multithreading
{
    // region → regionkey
    int regionKey = -1;
    for (size_t i = 0; i < region_data.r_name.size(); ++i) {
        if (region_data.r_name[i] == r_name) {
            regionKey = region_data.r_regionkey[i];
            break;
        }
    }
    if (regionKey == -1) return false;

    // nationkey → nation_name 
    std::unordered_map<int,std::string> nationkey_to_name;
    nationkey_to_name.reserve(nation_data.n_nationkey.size());

    for (size_t i = 0; i < nation_data.n_nationkey.size(); ++i) {
        if (nation_data.n_regionkey[i] == regionKey) {
            nationkey_to_name[nation_data.n_nationkey[i]] =
                nation_data.n_name[i];
        }
    }

    // Multithreaded suppkey → nationkey 
    num_threads = Config().num_threads;
    size_t total = supplier_data.s_suppkey.size();
    size_t chunk = total / num_threads;

    std::vector<std::unordered_map<int,int>> local_supp(num_threads);
    std::vector<std::thread> threads;

    for(int t=0;t<num_threads;++t){
        size_t s = t*chunk;
        size_t e = (t==num_threads-1)? total : s+chunk;

        threads.emplace_back(
            supplier_worker,
            s, e,
            std::cref(supplier_data),
            std::cref(nationkey_to_name),
            std::ref(local_supp[t])
        );
    }
    for(auto& th:threads) th.join();

    std::unordered_map<int,int> supp_to_nation;
    for(auto& m: local_supp)
        supp_to_nation.insert(m.begin(), m.end());



    // custkey → nationkey

    num_threads = Config().num_threads;
    total = customer_data.size();
    size_t chunk_size = total / num_threads;

    threads = std::vector<std::thread>();
    threads.reserve(num_threads);

    std::vector<std::unordered_map<int, int>> local_cust_maps(num_threads);

    // Optional but helpful
    for (auto& m : local_cust_maps)
        m.reserve(chunk_size / 2);

    for (int t = 0; t < num_threads; ++t) {
        size_t start = t * chunk_size;
        size_t end = (t == num_threads - 1)
                    ? total
                    : start + chunk_size;

        threads.emplace_back(
            customer_worker,
            start,
            end,
            std::cref(customer_data),
            std::ref(local_cust_maps[t])
        );
    }

    for (auto& th : threads)
        th.join();

    // Merge
    std::unordered_map<int, int> cust_to_nation;
    for (const auto& local_map : local_cust_maps) {
        for (const auto& kv : local_map) {
            cust_to_nation.emplace(kv);
        }
    }

    //  Multithreaded orderkey → nationkey 
    num_threads = g_config.num_threads;
    total = orders_data.size();
    chunk_size = total / num_threads;
    threads = std::vector<std::thread>();
    threads.reserve(num_threads);

    std::vector<std::unordered_map<int, int>> local_order_maps(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        size_t start = t * chunk_size;
        size_t end = (t == num_threads - 1)
                    ? total
                    : start + chunk_size;

        threads.emplace_back(
            orders_worker,
            start,
            end,
            std::cref(orders_data),
            std::cref(cust_to_nation),
            std::cref(start_date),
            std::cref(end_date),
            std::ref(local_order_maps[t])
        );
    }

    for (auto& th : threads)
        th.join();

    // Merge
    std::unordered_map<int,int> order_to_nation;
    for (const auto& local_map : local_order_maps) {
        for (const auto& kv : local_map) {
            order_to_nation.emplace(kv);
        }
    }


    // Multithreaded lineitem scan 
    num_threads = g_config.num_threads;
    size_t total_rows = lineitem_data.size();
    threads = std::vector<std::thread>();
    chunk_size = total_rows / num_threads;
    std::vector<std::unordered_map<std::string, double>> local_results(num_threads);


    for (int t = 0; t < num_threads; ++t) {
        size_t start = t * chunk_size;
        size_t end = (t == num_threads - 1)
                    ? total_rows
                    : start + chunk_size;

        threads.emplace_back(
            lineitem_worker,
            start,
            end,
            std::cref(lineitem_data),
            std::cref(order_to_nation),
            std::cref(supp_to_nation),
            std::cref(nationkey_to_name),
            std::ref(local_results[t])
        );
    }

    for (auto& th : threads)
        th.join();

    // Merge results 
    for (const auto& local_map : local_results) {
        for (const auto& [nation, revenue] : local_map) {
            results[nation] += revenue;
        }
    }

    return true;
}


bool comparator(const std::pair<std::string, double>& a, const std::pair<std::string, double>& b) {
    return a.second > b.second;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) 
    // TODO: Implement outputting results to a file
 {
    std::ofstream out(result_path + "\\query5_result.txt");
    if (!out.is_open()) {
        std::cerr << "Failed to open result file at: "
                  << result_path << std::endl;
        return false;
    }
    out << std::fixed << std::setprecision(2);
    // Write header
    out << "n_name|revenue\n";


    // Sort results by revenue descending
    std::vector<std::pair<std::string, double>> sorted_results(results.begin(), results.end());
    std::sort(sorted_results.begin(), sorted_results.end(), comparator);

    for (const auto& it : sorted_results) {
        out << it.first << "|" << it.second << "\n";
    }

    out.close();
    return true;
}