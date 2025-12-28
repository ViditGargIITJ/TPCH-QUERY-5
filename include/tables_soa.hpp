#pragma once
#include <vector>
#include<memory>
#include <string>
#include <sstream>

class tables {
public:
    virtual ~tables() = default;

    // Parse one line into columns
    virtual void insert_line(const std::string& line) = 0;

    // Create an empty object of the same derived type
    virtual std::unique_ptr<tables> create_empty() const = 0;

    // Merge another instance's data into this one
    virtual void merge_from(const tables& other) = 0;

    virtual int size() const = 0;

};



class CustomerSOA : public tables {
public:
    std::vector<int> c_custkey;
    std::vector<int> c_nationkey;

    void insert_line(const std::string& line) override {
        std::istringstream ss(line);
        std::string value;
        size_t index = 0;

        int custkey = 0;
        int nationkey = 0;

        while (std::getline(ss, value, '|') && index <= 3) {
            if (index == 0) custkey = std::stoi(value);
            else if (index == 3) nationkey = std::stoi(value);
            index++;
        }

        c_custkey.push_back(custkey);
        c_nationkey.push_back(nationkey);
    }

    std::unique_ptr<tables> create_empty() const override {
        return std::make_unique<CustomerSOA>();
    }

    void merge_from(const tables& other_base) override {
        const auto& other = dynamic_cast<const CustomerSOA&>(other_base);

        c_custkey.insert(c_custkey.end(),
                         other.c_custkey.begin(),
                         other.c_custkey.end());

        c_nationkey.insert(c_nationkey.end(),
                           other.c_nationkey.begin(),
                           other.c_nationkey.end());
    }
     
    int size() const {
        return c_custkey.size();
    }
};


class OrdersSOA : public tables {
public:
    std::vector<int> o_orderkey;
    std::vector<int> o_custkey;
    std::vector<std::string> o_orderdate;

    void insert_line(const std::string& line) override {
        std::istringstream ss(line);    
        std::string value;
        size_t index = 0;

        while (std::getline(ss, value, '|') && index <= 4) {
            if (index == 0)
                o_orderkey.push_back(std::stoi(value));
            else if (index == 1)
                o_custkey.push_back(std::stoi(value));
            else if (index == 4)
                o_orderdate.push_back(value);

            index++;
        }
    }

    std::unique_ptr<tables> create_empty() const override {
        return std::make_unique<OrdersSOA>();
    }

    void merge_from(const tables& other_base) override {
        const auto& other = dynamic_cast<const OrdersSOA&>(other_base);
        o_orderkey.insert(o_orderkey.end(),
                         other.o_orderkey.begin(), other.o_orderkey.end());
        o_custkey.insert(o_custkey.end(),
                         other.o_custkey.begin(), other.o_custkey.end());
        o_orderdate.insert(o_orderdate.end(),
                           other.o_orderdate.begin(), other.o_orderdate.end());
    }

    int size() const {
        return o_orderkey.size();
    }
};


class SupplierSOA : public tables {
public:
    std::vector<int> s_suppkey;
    std::vector<int> s_nationkey;

    void insert_line(const std::string& line) override {
        std::istringstream ss(line);
        std::string value;
        size_t index = 0;

        while (std::getline(ss, value, '|') && index <= 3) {
            if (index == 0)
                s_suppkey.push_back(std::stoi(value));
            else if (index == 3)
                s_nationkey.push_back(std::stoi(value));

            index++;
        }
    }

    std::unique_ptr<tables> create_empty() const override {
        return std::make_unique<SupplierSOA>();
    }

    void merge_from(const tables& other_base) override {
        const auto& other = dynamic_cast<const SupplierSOA&>(other_base);
        s_suppkey.insert(s_suppkey.end(),
                         other.s_suppkey.begin(), other.s_suppkey.end());
        s_nationkey.insert(s_nationkey.end(),
                           other.s_nationkey.begin(), other.s_nationkey.end());
    }
    int size() const {
        return s_suppkey.size();
    }
};

class RegionSOA : public tables {
public:
    std::vector<int> r_regionkey;
    std::vector<std::string> r_name;

    void insert_line(const std::string& line) override {
        std::istringstream ss(line);
        std::string value;
        size_t index = 0;

        while (std::getline(ss, value, '|') && index <= 1) {
            if (index == 0)
                r_regionkey.push_back(std::stoi(value));
            else if (index == 1)
                r_name.push_back(value);

            index++;
        }
    }

    std::unique_ptr<tables> create_empty() const override {
        return std::make_unique<RegionSOA>();
    }

    void merge_from(const tables& other_base) override {
        const auto& other = dynamic_cast<const RegionSOA&>(other_base);
        r_regionkey.insert(r_regionkey.end(),
                           other.r_regionkey.begin(), other.r_regionkey.end());
        r_name.insert(r_name.end(),
                      other.r_name.begin(), other.r_name.end());
    }
    int size() const {
        return r_regionkey.size();
    }
};

class NationSOA : public tables {
public:
    std::vector<int> n_nationkey;
    std::vector<int> n_regionkey;
    std::vector<std::string> n_name;

    void insert_line(const std::string& line) override {
        std::istringstream ss(line);
        std::string value;
        size_t index = 0;

        int nationkey = 0;
        int regionkey = 0;
        std::string name;

        while (std::getline(ss, value, '|') && index <= 2) {
            if (index == 0)
                nationkey = std::stoi(value);
            else if (index == 1)
                name = value;
            else if (index == 2)
                regionkey = std::stoi(value);

            index++;
        }

        n_nationkey.push_back(nationkey);
        n_name.push_back(name);
        n_regionkey.push_back(regionkey);
    }

    std::unique_ptr<tables> create_empty() const override {
        return std::make_unique<NationSOA>();
    }

    void merge_from(const tables& other_base) override {
        const auto& other = dynamic_cast<const NationSOA&>(other_base);

        n_nationkey.insert(n_nationkey.end(),
                           other.n_nationkey.begin(), other.n_nationkey.end());

        n_regionkey.insert(n_regionkey.end(),
                           other.n_regionkey.begin(), other.n_regionkey.end());

        n_name.insert(n_name.end(),
                      other.n_name.begin(), other.n_name.end());
    }

    int size() const {
        return (int)n_nationkey.size();
    }
};


class LineItemSOA : public tables {
public:
    std::vector<int>    l_orderkey;
    std::vector<int>    l_suppkey;
    std::vector<double> l_extendedprice;
    std::vector<double> l_discount;

    void insert_line(const std::string& line) override {
        std::istringstream ss(line);
        std::string value;
        size_t index = 0;

        while (std::getline(ss, value, '|') && index <= 6) {
            if (index == 0)
                l_orderkey.push_back(std::stoi(value));
            else if (index == 2)
                l_suppkey.push_back(std::stoi(value));
            else if (index == 5)
                l_extendedprice.push_back(std::stod(value));
            else if (index == 6)
                l_discount.push_back(std::stod(value));

            index++;
        }

        
    }

    std::unique_ptr<tables> create_empty() const override {
        return std::make_unique<LineItemSOA>();
    }

    void merge_from(const tables& other_base) override {
        const auto& other = dynamic_cast<const LineItemSOA&>(other_base);   
        l_orderkey.insert(l_orderkey.end(),
                         other.l_orderkey.begin(), other.l_orderkey.end());
        l_suppkey.insert(l_suppkey.end(),
                         other.l_suppkey.begin(), other.l_suppkey.end());
        l_extendedprice.insert(l_extendedprice.end(),
                              other.l_extendedprice.begin(), other.l_extendedprice.end());
        l_discount.insert(l_discount.end(),
                         other.l_discount.begin(), other.l_discount.end());
    }
    
    int size() const {
            return l_orderkey.size();
    }

};


