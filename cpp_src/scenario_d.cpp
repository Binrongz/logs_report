/**
 * Scenario D: Rule-Based Log Analysis with HPC Acceleration
 * 
 * Purpose: Fast log processing using simple rules and OpenMP parallelization
 * Focus: Throughput and scalability, not accuracy
 * 
 * Compile: make
 * Run: ./scenario_d data/subset_500.csv output/ 32
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <map>
#include <set>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <cmath>
#include <omp.h>
#include <sys/resource.h>

using namespace std;

// ============================================================================
// Data Structures
// ============================================================================

struct LogEntry {
    int line_id;
    string label;              // Ground truth
    string timestamp;
    string date;
    string node;
    string time;
    string component;
    string level;
    string content;
    string event_template;
    
    // Analysis results
    string predicted_label;
    string confidence;
    string severity_level;
    vector<string> keywords;
    string affected_component;
    string issue_category;
    
    // Performance metrics
    double stage1_time_ms;
    double stage2_time_ms;
    double total_time_ms;
};

struct PerformanceStats {
    int total_logs;
    int num_threads;
    double total_time_sec;
    double stage1_time_sec;
    double stage2_time_sec;
    double throughput_logs_per_sec;
    double avg_time_per_log_ms;
    double stage1_percentage;
    double stage2_percentage;
    int correct_predictions;
    double accuracy_percentage;
    double avg_keywords_count;
    double avg_keywords_chars;
    long peak_memory_mb;
};

// ============================================================================
// Rule Engine (Stage 1)
// ============================================================================

class RuleEngine {
private:
    map<string, set<string>> label_rules;
    
public:
    RuleEngine() {
        initializeRules();
    }
    
    void analyze(LogEntry& log) {
        auto start = chrono::high_resolution_clock::now();
        
        // Extract keywords
        log.keywords = extractKeywords(log.content);
        
        // Classify
        log.predicted_label = classify(log.keywords, log.level, log.content);
        
        // Calculate confidence
        log.confidence = calculateConfidence(log.keywords, log.predicted_label);
        
        // Determine severity
        log.severity_level = determineSeverity(log.level);
        
        // Other fields
        log.affected_component = log.component;
        log.issue_category = categorize(log.keywords);
        
        auto end = chrono::high_resolution_clock::now();
        log.stage1_time_ms = chrono::duration<double, milli>(end - start).count();
    }
    
private:
    void initializeRules() {
        // Network-related issues
        label_rules["Network"] = {
            "connection", "timeout", "network", "socket",
            "refused", "unreachable", "dns", "port", "link"
        };
        
        // Resource-related issues
        label_rules["Resource"] = {
            "memory", "cpu", "disk", "allocation", "limit",
            "exceeded", "usage", "capacity", "resource"
        };
        
        // Security-related issues
        label_rules["Security"] = {
            "authentication", "permission", "denied", "unauthorized",
            "access", "login", "credential", "security", "auth"
        };
        
        // Hardware-related issues
        label_rules["Hardware"] = {
            "hardware", "device", "driver", "firmware", "physical"
        };
        
        // Application-related issues
        label_rules["Application"] = {
            "error", "exception", "failed", "crash", "abort",
            "core", "fault", "fatal", "panic", "signal"
        };
    }
    
    vector<string> extractKeywords(const string& content) {
        vector<string> keywords;
        string lower_content = toLowerCase(content);
        
        // Simple tokenization
        istringstream iss(lower_content);
        string word;
        while (iss >> word) {
            // Remove punctuation
            word.erase(
                remove_if(word.begin(), word.end(), 
                         [](char c) { return !isalnum(c); }), 
                word.end()
            );
            
            // Keep words longer than 2 characters
            if (word.length() > 2) {
                keywords.push_back(word);
            }
        }
        
        // Remove duplicates
        sort(keywords.begin(), keywords.end());
        keywords.erase(unique(keywords.begin(), keywords.end()), keywords.end());
        
        // Limit to top 10
        if (keywords.size() > 10) {
            keywords.resize(10);
        }
        
        return keywords;
    }
    
    string classify(const vector<string>& keywords, const string& level, 
                   const string& content) {
        map<string, int> scores;
        
        // Calculate matching scores for each label
        for (const auto& [label, rules] : label_rules) {
            int score = 0;
            for (const auto& kw : keywords) {
                for (const auto& rule : rules) {
                    if (kw.find(rule) != string::npos || 
                        rule.find(kw) != string::npos) {
                        score++;
                    }
                }
            }
            scores[label] = score;
        }
        
        // Find best match
        string best_label = "-";
        int max_score = 0;
        for (const auto& [label, score] : scores) {
            if (score > max_score) {
                max_score = score;
                best_label = label;
            }
        }
        
        // If no keywords matched and level is INFO, it's normal
        if (max_score == 0 && level == "INFO") {
            return "-";
        }
        
        // Even with low score, if INFO level, likely normal
        if (max_score <= 1 && level == "INFO") {
            return "-";
        }
        
        return best_label;
    }
    
    string calculateConfidence(const vector<string>& keywords, 
                              const string& label) {
        if (label == "-") {
            // Check if there are any problem keywords
            bool has_problem = false;
            for (const auto& [lbl, rules] : label_rules) {
                for (const auto& kw : keywords) {
                    for (const auto& rule : rules) {
                        if (kw.find(rule) != string::npos) {
                            has_problem = true;
                            break;
                        }
                    }
                    if (has_problem) break;
                }
                if (has_problem) break;
            }
            return has_problem ? "low" : "high";
        }
        
        // For problem categories
        int match_count = 0;
        if (label_rules.count(label)) {
            for (const auto& kw : keywords) {
                for (const auto& rule : label_rules.at(label)) {
                    if (kw.find(rule) != string::npos) {
                        match_count++;
                        break;
                    }
                }
            }
        }
        
        if (match_count >= 3) return "high";
        if (match_count >= 1) return "medium";
        return "low";
    }
    
    string determineSeverity(const string& level) {
        if (level == "CRITICAL" || level == "FATAL") return "CRITICAL";
        if (level == "ERROR") return "ERROR";
        if (level == "WARN" || level == "WARNING") return "WARNING";
        return "INFO";
    }
    
    string categorize(const vector<string>& keywords) {
        for (const auto& kw : keywords) {
            if (kw.find("config") != string::npos) return "Configuration";
            if (kw.find("perform") != string::npos) return "Performance";
            if (kw.find("connect") != string::npos) return "Connectivity";
        }
        return "General";
    }
    
    string toLowerCase(string s) {
        transform(s.begin(), s.end(), s.begin(), ::tolower);
        return s;
    }
};

// ============================================================================
// Report Generator (Stage 2)
// ============================================================================

class ReportGenerator {
public:
    void generate(LogEntry& log) {
        auto start = chrono::high_resolution_clock::now();
        
        // Simple report generation (just timing, no actual text needed)
        // In real implementation, would create formatted report
        
        auto end = chrono::high_resolution_clock::now();
        log.stage2_time_ms = chrono::duration<double, milli>(end - start).count();
    }
};

// ============================================================================
// CSV Parser
// ============================================================================

vector<LogEntry> loadCSV(const string& filename) {
    vector<LogEntry> logs;
    ifstream file(filename);
    
    if (!file.is_open()) {
        cerr << "Error: Cannot open file " << filename << endl;
        return logs;
    }
    
    string line;
    // Skip header
    getline(file, line);
    
    int line_count = 0;
    while (getline(file, line)) {
        if (line.empty()) continue;
        
        line_count++;
        LogEntry log;
        stringstream ss(line);
        string field;
        
        try {
            // Parse CSV: LineId,Label,Timestamp,Date,Node,Time,NodeRepeat,Type,Component,Level,Content,EventId,EventTemplate
            getline(ss, field, ','); 
            log.line_id = stoi(field);
            
            getline(ss, field, ','); 
            log.label = field;
            
            getline(ss, field, ','); 
            log.timestamp = field;
            
            getline(ss, field, ','); 
            log.date = field;
            
            getline(ss, field, ','); 
            log.node = field;
            
            getline(ss, field, ','); 
            log.time = field;
            
            getline(ss, field, ','); // NodeRepeat (skip)
            getline(ss, field, ','); // Type (skip)
            
            getline(ss, field, ','); 
            log.component = field;
            
            getline(ss, field, ','); 
            log.level = field;
            
            getline(ss, field, ','); 
            log.content = field;
            
            getline(ss, field, ','); // EventId (skip)
            
            getline(ss, field, ','); 
            log.event_template = field;
            
            logs.push_back(log);
            
        } catch (const exception& e) {
            cerr << "Warning: Failed to parse line " << line_count << ": " << e.what() << endl;
            continue;
        }
    }
    
    cout << "Loaded " << logs.size() << " logs from " << filename << endl;
    return logs;
}

// ============================================================================
// Performance Statistics
// ============================================================================

PerformanceStats calculateStats(const vector<LogEntry>& logs, 
                                double total_time_sec,
                                int num_threads) {
    PerformanceStats stats;
    
    stats.total_logs = logs.size();
    stats.num_threads = num_threads;
    stats.total_time_sec = total_time_sec;
    
    double sum_stage1 = 0, sum_stage2 = 0;
    int total_keywords = 0, total_keyword_chars = 0;
    int correct = 0;
    
    for (const auto& log : logs) {
        sum_stage1 += log.stage1_time_ms;
        sum_stage2 += log.stage2_time_ms;
        
        total_keywords += log.keywords.size();
        for (const auto& kw : log.keywords) {
            total_keyword_chars += kw.length();
        }
        
        if (log.predicted_label == log.label) {
            correct++;
        }
    }
    
    stats.stage1_time_sec = sum_stage1 / 1000.0;
    stats.stage2_time_sec = sum_stage2 / 1000.0;
    stats.throughput_logs_per_sec = logs.size() / total_time_sec;
    stats.avg_time_per_log_ms = (sum_stage1 + sum_stage2) / logs.size();
    
    double total_stage_time = stats.stage1_time_sec + stats.stage2_time_sec;
    if (total_stage_time > 0) {
        stats.stage1_percentage = (stats.stage1_time_sec / total_stage_time) * 100.0;
        stats.stage2_percentage = (stats.stage2_time_sec / total_stage_time) * 100.0;
    }
    
    stats.correct_predictions = correct;
    stats.accuracy_percentage = (100.0 * correct) / logs.size();
    
    stats.avg_keywords_count = (double)total_keywords / logs.size();
    stats.avg_keywords_chars = (double)total_keyword_chars / logs.size();
    
    return stats;
}

void printStats(const PerformanceStats& stats) {
    cout << "\n" << string(80, '=') << endl;
    cout << "PERFORMANCE ANALYSIS SUMMARY" << endl;
    cout << string(80, '=') << endl;
    
    cout << "\n--- Overall Throughput ---" << endl;
    cout << "Total logs: " << stats.total_logs << endl;
    cout << "Threads: " << stats.num_threads << endl;
    cout << "Total time: " << fixed << setprecision(3) << stats.total_time_sec << " seconds" << endl;
    cout << "Throughput: " << fixed << setprecision(2) << stats.throughput_logs_per_sec << " logs/sec" << endl;
    cout << "Avg time per log: " << fixed << setprecision(3) << stats.avg_time_per_log_ms << " ms" << endl;
    
    cout << "\n--- Stage Breakdown ---" << endl;
    cout << "Stage 1: " << fixed << setprecision(3) << stats.stage1_time_sec << "s (" 
         << fixed << setprecision(1) << stats.stage1_percentage << "%)" << endl;
    cout << "Stage 2: " << fixed << setprecision(3) << stats.stage2_time_sec << "s (" 
         << fixed << setprecision(1) << stats.stage2_percentage << "%)" << endl;
    
    cout << "\n--- Prediction Accuracy ---" << endl;
    cout << "Correct: " << stats.correct_predictions << "/" << stats.total_logs << endl;
    cout << "Accuracy: " << fixed << setprecision(1) << stats.accuracy_percentage << "%" << endl;
    
    cout << "\n--- Keywords Statistics ---" << endl;
    cout << "Avg keywords per log: " << fixed << setprecision(1) << stats.avg_keywords_count << endl;
    cout << "Avg chars per log: " << fixed << setprecision(1) << stats.avg_keywords_chars << endl;
    
    cout << "\n--- Memory Usage ---" << endl;
    cout << "Peak memory: " << stats.peak_memory_mb << " MB" << endl;

    cout << string(80, '=') << endl;
}

void saveStatsJSON(const PerformanceStats& stats, const string& filename) {
    ofstream out(filename);
    
    out << "{\n";
    out << "  \"metadata\": {\n";
    out << "    \"scenario\": \"scenario_d\",\n";
    out << "    \"total_logs_processed\": " << stats.total_logs << ",\n";
    out << "    \"num_threads\": " << stats.num_threads << ",\n";
    out << "    \"total_time_seconds\": " << fixed << setprecision(6) << stats.total_time_sec << "\n";
    out << "  },\n";
    out << "  \"throughput\": {\n";
    out << "    \"logs_per_second\": " << fixed << setprecision(3) << stats.throughput_logs_per_sec << ",\n";
    out << "    \"avg_time_per_log_ms\": " << fixed << setprecision(3) << stats.avg_time_per_log_ms << "\n";
    out << "  },\n";
    out << "  \"stage_breakdown\": {\n";
    out << "    \"stage1_time_sec\": " << fixed << setprecision(6) << stats.stage1_time_sec << ",\n";
    out << "    \"stage2_time_sec\": " << fixed << setprecision(6) << stats.stage2_time_sec << ",\n";
    out << "    \"stage1_percentage\": " << fixed << setprecision(2) << stats.stage1_percentage << ",\n";
    out << "    \"stage2_percentage\": " << fixed << setprecision(2) << stats.stage2_percentage << "\n";
    out << "  },\n";
    out << "  \"accuracy\": {\n";
    out << "    \"correct\": " << stats.correct_predictions << ",\n";
    out << "    \"total\": " << stats.total_logs << ",\n";
    out << "    \"accuracy_percentage\": " << fixed << setprecision(2) << stats.accuracy_percentage << "\n";
    out << "  },\n";
    out << "  \"keywords_statistics\": {\n";
    out << "    \"avg_keywords_count\": " << fixed << setprecision(2) << stats.avg_keywords_count << ",\n";
    out << "    \"avg_keywords_chars\": " << fixed << setprecision(2) << stats.avg_keywords_chars << "\n";
    out << "  },\n";
    out << "  \"memory_usage\": {\n";
    out << "    \"peak_memory_mb\": " << stats.peak_memory_mb << "\n";
    out << "  }\n";
    out << "}\n";
    
    cout << "\nPerformance stats saved to: " << filename << endl;
}

void saveDetailedResults(const vector<LogEntry>& logs, const string& filename) {
    ofstream out(filename);
    
    out << "LineId,GroundTruth,PredictedLabel,Confidence,Severity,"
        << "Stage1TimeMs,Stage2TimeMs,TotalTimeMs,KeywordsCount\n";
    
    for (const auto& log : logs) {
        out << log.line_id << ","
            << log.label << ","
            << log.predicted_label << ","
            << log.confidence << ","
            << log.severity_level << ","
            << fixed << setprecision(3) << log.stage1_time_ms << ","
            << fixed << setprecision(3) << log.stage2_time_ms << ","
            << fixed << setprecision(3) << (log.stage1_time_ms + log.stage2_time_ms) << ","
            << log.keywords.size() << "\n";
    }
    
    cout << "Detailed results saved to: " << filename << endl;
}

void printLabelDistribution(const vector<LogEntry>& logs) {
    map<string, int> ground_truth_dist;
    map<string, int> predicted_dist;
    
    for (const auto& log : logs) {
        ground_truth_dist[log.label]++;
        predicted_dist[log.predicted_label]++;
    }
    
    cout << "\n--- Label Distribution ---" << endl;
    
    cout << "\nGround Truth:" << endl;
    for (const auto& [label, count] : ground_truth_dist) {
        cout << "  " << (label.empty() || label == "-" ? "Normal (-)" : label) 
             << ": " << count << endl;
    }
    
    cout << "\nPredicted:" << endl;
    for (const auto& [label, count] : predicted_dist) {
        cout << "  " << (label.empty() || label == "-" ? "Normal (-)" : label) 
             << ": " << count << endl;
    }
}

// ============================================================================
// Main Program
// ============================================================================

int main(int argc, char* argv[]) {
    // Parse arguments
    string input_file = "data/subset_500.csv";
    string output_dir = "output/";
    int num_threads = 32;
    
    if (argc > 1) input_file = argv[1];
    if (argc > 2) output_dir = argv[2];
    if (argc > 3) num_threads = stoi(argv[3]);
    
    // Ensure output directory ends with /
    if (!output_dir.empty() && output_dir.back() != '/') {
        output_dir += '/';
    }
    
    cout << string(80, '=') << endl;
    cout << "SCENARIO D: C++ HPC LOG ANALYSIS" << endl;
    cout << string(80, '=') << endl;
    cout << "Input: " << input_file << endl;
    cout << "Output: " << output_dir << endl;
    cout << "Threads: " << num_threads << endl;
    cout << string(80, '=') << endl;
    
    // Load data
    cout << "\n[1/4] Loading dataset..." << endl;
    vector<LogEntry> logs = loadCSV(input_file);
    
    if (logs.empty()) {
        cerr << "No logs loaded. Exiting." << endl;
        return 1;
    }
    
    // Initialize engines
    cout << "\n[2/4] Initializing engines..." << endl;
    RuleEngine rule_engine;
    ReportGenerator report_gen;
    cout << "Engines initialized" << endl;
    
    // Set parallelization
    omp_set_num_threads(num_threads);
    cout << "OpenMP threads: " << num_threads << endl;
    
    // Process logs
    cout << "\n[3/4] Processing logs..." << endl;
    auto total_start = chrono::high_resolution_clock::now();
    
    #pragma omp parallel for schedule(dynamic, 10)
    for (size_t i = 0; i < logs.size(); i++) {
        // Stage 1: Rule-based analysis
        rule_engine.analyze(logs[i]);
        
        // Stage 2: Report generation
        report_gen.generate(logs[i]);
        
        // Calculate total time
        logs[i].total_time_ms = logs[i].stage1_time_ms + logs[i].stage2_time_ms;
        
        // Progress display (every 100 logs)
        if (i % 100 == 0 && i > 0) {
            #pragma omp critical
            {
                cout << "  Processed: " << i << "/" << logs.size() << endl;
            }
        }
    }
    
    auto total_end = chrono::high_resolution_clock::now();
    double total_time = chrono::duration<double>(total_end - total_start).count();
    
    cout << "Processing completed!" << endl;
    
    // Calculate and print statistics
    cout << "\n[4/4] Calculating statistics..." << endl;
    PerformanceStats stats = calculateStats(logs, total_time, num_threads);
    
    // Print statistics
    printStats(stats);
    
    // Print label distribution
    printLabelDistribution(logs);

    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    #ifdef __APPLE__
        long memory_mb = usage.ru_maxrss / (1024 * 1024);  // Mac
    #else
        long memory_mb = usage.ru_maxrss / 1024;            // Linux
    #endif

    cout << "\n--- Memory Usage ---" << endl;
    cout << "Peak memory: " << memory_mb << " MB" << endl;
    
    // Save results
    cout << "\n--- Saving Results ---" << endl;
    saveStatsJSON(stats, output_dir + "scenario_d_performance.json");
    saveDetailedResults(logs, output_dir + "scenario_d_results.csv");
    
    cout << "\n" << string(80, '=') << endl;
    cout << "EXPERIMENT COMPLETED" << endl;
    cout << string(80, '=') << endl;
    
    
    return 0;
}