
//====================================================================
//  BloodlineCalculator  ―― 1 頭と全頭の血量を高速出力
//    ・File‑A : 行 = 全馬, 列 = 対象馬 (対象馬の血が何 % 含まれるか)
//    ・File‑B : 行 = 対象馬, 列 = 全馬 (各馬の血が何 % 含まれるか)
//      └─ 列は 1 つだけ、行は全馬            （縦長フォーマット）
//    ── 無縁のペアは計算せず 0.0 を書くので高速 & 低メモリ
//====================================================================
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <vector>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <tuple>
#include <algorithm>
#include <climits>
#include <cmath>
#define NOMINMAX    // これを windows.h より前に置く
#include <windows.h>
#include <psapi.h>

#ifdef _WIN32
#include <windows.h>
#include <psapi.h>
#else
#include <sys/resource.h>
#include <unistd.h>
#endif
#include <rocksdb/db.h>
#include <rocksdb/options.h>
// --------------- 追加ヘッダ ----------------
#include <regex>

// --------------- 追加 util -----------------
static inline std::string trim(std::string s) {
    size_t b = s.find_first_not_of(" \t\r\n");
    size_t e = s.find_last_not_of(" \t\r\n");
    return (b == std::string::npos) ? "" : s.substr(b, e - b + 1);
}

std::unique_ptr<rocksdb::DB> db;          // メモ化キャッシュ本体
constexpr const char* DBPATH = "./bloodcache_db";

//------------------------- 基本定数 -------------------------------
static const std::string UNKNOWN_SIRE = "UNKNOWN_SIRE";
static const std::string UNKNOWN_DAM = "UNKNOWN_DAM";
static const size_t MEMORY_THRESHOLD_MB = 10'000;   // 10 GB で警告

//------------------------- 構造体 -------------------------------
struct Horse {
    std::string PrimaryKey;
    std::string HorseName;
    std::string YearStr;
    int         YearInt;
    std::string Sire;
    std::string Dam;
};

//------------------------- 大域データ -------------------------------
std::unordered_map<std::string, Horse> horses;        // PK → 馬
std::unordered_map<std::string, std::string> keyToDisplayName;
std::unordered_map<std::string,
    std::unordered_map<std::string, double>> dp; // メモ化
std::unordered_map<std::string, std::vector<std::string>> childrenOf; // 親→子

//------------------------- ユーティリティ -------------------------------
size_t getMemoryUsageMB() {
#ifdef _WIN32
    PROCESS_MEMORY_COUNTERS_EX pmc;
    if (GetProcessMemoryInfo(GetCurrentProcess(),
        (PROCESS_MEMORY_COUNTERS*)&pmc,
        sizeof(pmc)))
        return pmc.WorkingSetSize / (1024 * 1024);
#else
    struct rusage u;            // mac / Linux
    if (!getrusage(RUSAGE_SELF, &u))
        return u.ru_maxrss / 1024;
#endif
    return 0;
}
int parseYearInt(const std::string& y) {
    if (y.empty()) return INT_MIN;
    try { return std::stoi(y); }
    catch (...) { return INT_MIN; }
}
std::vector<std::string> splitCSV(const std::string& line) {
    std::vector<std::string> out;  bool inq = false;  std::string buf;
    for (char c : line) {
        if (c == '"') { inq = !inq; }
        else if (c == ',' && !inq) { out.push_back(buf); buf.clear(); }
        else buf.push_back(c);
    }
    out.push_back(buf); return out;
}
void openDB()
{
    rocksdb::Options op;
    op.create_if_missing = true;
    op.compression = rocksdb::kNoCompression;   // ← これに変更

    op.IncreaseParallelism();
    op.OptimizeLevelStyleCompaction();
    rocksdb::DB* raw = nullptr;
    auto st = rocksdb::DB::Open(op, DBPATH, &raw);
    if (!st.ok()) { std::cerr << st.ToString() << '\n'; exit(1); }
    db.reset(raw);
}
constexpr size_t LRU_LIMIT = 200'000;          // 好きなサイズ
struct LRUKey { std::string tgt, anc; };
struct KeyHash {
    size_t operator()(const LRUKey& k) const {
        return std::hash<std::string>()(k.tgt) ^ std::hash<std::string>()(k.anc) << 1;
    }
};
struct KeyEq {
    bool operator()(const LRUKey& a, const LRUKey& b) const {
        return a.tgt == b.tgt && a.anc == b.anc;
    }
};

std::unordered_map<LRUKey, double, KeyHash, KeyEq> lru;
std::deque<LRUKey> order;          // push_back / pop_front で単純 LRU
void lruPut(const LRUKey& k, double v) {
    lru[k] = v; order.push_back(k);
    if (order.size() > LRU_LIMIT) {
        lru.erase(order.front()); order.pop_front();
    }
}
bool lruGet(const LRUKey& k, double& v) {
    auto it = lru.find(k); if (it == lru.end()) return false;
    v = it->second; return true;
}

//------------------------- CSV 読込 -------------------------------
void loadBloodlineCSV(const std::string& f) {
    std::ifstream ifs(f);  if (!ifs) { std::cerr << "cannot open " << f << '\n'; exit(1); }
    std::string head; getline(ifs, head);
    std::string ln;  int cnt = 0;
    while (getline(ifs, ln)) {
        if (ln.empty()) continue;
        auto c = splitCSV(ln); if (c.size() < 9) continue;
        Horse h;
        h.PrimaryKey = c[0];
        h.Sire = c[1].empty() ? UNKNOWN_SIRE : c[1];
        h.Dam = c[2].empty() ? UNKNOWN_DAM : c[2];
        h.YearStr = c[5];  h.YearInt = parseYearInt(c[5]);
        h.HorseName = c[8];
        horses[h.PrimaryKey] = h;
        keyToDisplayName[h.PrimaryKey] = h.HorseName + " [" + h.YearStr + "]";
        childrenOf[h.Sire].push_back(h.PrimaryKey);
        childrenOf[h.Dam].push_back(h.PrimaryKey);
        ++cnt;
    }
    std::cout << "[load] " << cnt << " rows, horses=" << horses.size() << '\n';
}
//------------------------- 血量計算（メモ化） -------------------------------
double getBlood(const std::string& tgt, const std::string& anc,
    std::unordered_set<std::string>& stk)
{
    LRUKey key{ tgt,anc };
    double val;

    // 1) LRU → RocksDB → 再計算 の 3 段階
    if (lruGet(key, val)) return val;

    std::string vstr;
    if (db->Get(rocksdb::ReadOptions(), tgt + "|" + anc, &vstr).ok()) {
        val = std::stod(vstr);
        lruPut(key, val);
        return val;
    }

    // ---- 以下は以前の再帰計算ロジック ----
    if (tgt == UNKNOWN_SIRE || tgt == UNKNOWN_DAM || !horses.count(tgt))
        val = 0.0;
    else if (tgt == anc) val = 1.0;
    else if (stk.count(tgt)) val = 0.0;
    else {
        stk.insert(tgt);
        const Horse& h = horses[tgt];
        val = 0.5 * getBlood(h.Sire, anc, stk) + 0.5 * getBlood(h.Dam, anc, stk);
        stk.erase(tgt);
    }

    // 2) RocksDB + LRU に保存
    db->Put(rocksdb::WriteOptions(), tgt + "|" + anc, std::to_string(val));
    lruPut(key, val);
    return val;
}

//------------------------- 祖先・子孫セット -------------------------------
void collectAncestors(const std::string& pk, std::unordered_set<std::string>& s) {
    if (pk == UNKNOWN_SIRE || pk == UNKNOWN_DAM || !horses.count(pk)) return;
    const Horse& h = horses[pk];
    for (const auto& p : { h.Sire,h.Dam })
        if (s.insert(p).second) collectAncestors(p, s);
}
void collectDescendants(const std::string& pk, std::unordered_set<std::string>& s) {
    std::queue<std::string> q; q.push(pk);
    while (!q.empty()) {
        std::string cur = q.front(); q.pop();
        for (const auto& ch : childrenOf[cur])
            if (s.insert(ch).second) q.push(ch);
    }
}
//--------------------------------------------------------------------
// 行列 CSV 出力  (transpose==true で行列を入れ替えて出力)
//
//  1. calcFilter(row, col) が true のペアだけ getBlood()
//     （例: 行が子孫集合に含まれる時だけ計算）
//  2. ログは [calc / skip] ＋ % 表示で統一
//--------------------------------------------------------------------
//--------------------------------------------------------------------
// 行列 CSV 出力  (transpose==true で行列を入れ替えて出力)
//--------------------------------------------------------------------
void saveCSVMatrix_Smart(const std::string& filename,
    const std::vector<std::string>& rowKeys,
    const std::vector<std::string>& colKeys,
    bool transpose,
    const std::function<bool(const std::string&,
        const std::string&)>& calcFilter)
{
    if (rowKeys.empty() || colKeys.empty()) {
        std::cerr << "[saveCSVMatrix] rows/cols empty → skip\n";
        return;
    }

    const auto& rows = transpose ? colKeys : rowKeys;  // ⇐ 行
    const auto& cols = transpose ? rowKeys : colKeys;  // ⇐ 列

    std::ofstream ofs(filename);
    if (!ofs) { std::cerr << "cannot open " << filename << '\n'; return; }
    ofs << std::fixed << std::setprecision(8);

    // --- ヘッダ ---
    ofs << "HorseName";
    for (const auto& ck : cols) ofs << ',' << keyToDisplayName[ck];
    ofs << '\n';

    // --- 本文 ---
    const size_t total = rows.size();
    size_t idx = 0;
    for (const auto& rk : rows) {
        ++idx;
        std::cout << "[Matrix] (" << idx << '/' << total << ")  "
            << keyToDisplayName[rk] << '\n';

        ofs << keyToDisplayName[rk];

        for (const auto& ck : cols) {
            bool need = calcFilter(transpose ? ck : rk,   // row/col を元順で渡す
                transpose ? rk : ck);

            double v = 0.0;
            if (need) {
                std::unordered_set<std::string> st;

                // ★ 行列を転置したときだけ引数の向きを入れ替える
                v = transpose
                    ? getBlood(ck, rk, st)   // descendant = ck, ancestor = rk
                    : getBlood(rk, ck, st);  // descendant = rk, ancestor = ck

                if (std::fabs(v) < 1e-12) v = 0.0;
            }
            ofs << ',' << v;
        }
        ofs << '\n';
    }
    std::cout << "[Matrix] " << filename << " 出力完了\n";
}




//-------------------------------------------------------------
// 出力 A  (行＝全馬, 列＝対象 1 頭)  ―― 進捗・値付き
//-------------------------------------------------------------
void saveDescFast(const std::string& out,
    const std::vector<std::string>& rows,
    const std::unordered_set<std::string>& setDesc,
    const std::string& target)
{
    std::ofstream ofs(out);
    ofs << std::fixed << std::setprecision(8);
    ofs << "HorseName," << keyToDisplayName[target] << '\n';

    const size_t total = rows.size();
    size_t idx = 0;
    for (const auto& rk : rows) {
        ++idx;
        double v = 0.0;
        bool needCalc = setDesc.count(rk);
        if (needCalc) {
            std::unordered_set<std::string> stk;
            v = getBlood(rk, target, stk);
            if (std::fabs(v) < 1e-12) v = 0.0;
        }
        //--------------- saveDescFast 内 ------------------------------------
        double v_pct = std::floor(v * 1'000'000.0) / 10'000.0;  // 5 桁切り捨て
        std::cout << "[A] (" << idx << '/' << total << ")  "
            << keyToDisplayName[rk] << "  "
            << (needCalc ? "[calc: " : "[skip: ")
            << std::fixed << std::setprecision(5) << v_pct << "%]\n";


        // ---- CSV 書き込み ----
        ofs << keyToDisplayName[rk] << ',' << std::setprecision(8) << v << '\n';
    }
}

//-------------------------------------------------------------
// 出力 B  (縦長 1 列)  ―― 進捗・値付き
//-------------------------------------------------------------
void saveAncVert(const std::string& out,
    const std::vector<std::string>& all,
    const std::unordered_set<std::string>& setAnc,
    const std::string& target)
{
    std::ofstream ofs(out);
    ofs << std::fixed << std::setprecision(8);
    ofs << "HorseName," << keyToDisplayName[target] << '\n';

    const size_t total = all.size();
    size_t idx = 0;
    for (const auto& anc : all) {
        ++idx;
        double v = 0.0;
        bool needCalc = setAnc.count(anc);
        if (needCalc) {
            std::unordered_set<std::string> stk;
            v = getBlood(target, anc, stk);
            if (std::fabs(v) < 1e-12) v = 0.0;
        }
        // ---- 進捗ログ ----
        double v_pct = std::floor(v * 1'000'000.0) / 10'000.0;  // 5 桁切り捨て
        std::cout << "[B] (" << idx << '/' << total << ")  "
            << keyToDisplayName[anc] << "  "
            << (needCalc ? "[calc: " : "[skip: ")
            << std::fixed << std::setprecision(5) << v_pct << "%]\n";

        // ---- CSV 書き込み ----
        ofs << keyToDisplayName[anc] << ',' << std::setprecision(8) << v << '\n';
    }
}
// =========================== main ===========================
int main() {
    loadBloodlineCSV("bloodline.csv");

    // --- 入力 ---
    std::cout << "対象馬 (年/年レンジ/PrimaryKey をカンマ区切り): ";
    std::string raw;  std::getline(std::cin, raw);

    openDB();                               // RocksDB を開く

    /* ---------- 1. 文字列を解析して targetPks を作成 ---------- */
    std::unordered_set<std::string> targetSet;
    std::vector<std::string>        idTokens;

    std::regex reRange(R"(^(\d{4})-(\d{4})$)");
    std::regex reYear(R"(^(\d{4})$)");

    std::stringstream ss(raw);
    for (std::string tok; std::getline(ss, tok, ','); ) {
        tok = trim(tok);
        if (tok.empty()) continue;
        idTokens.push_back(tok);

        std::smatch m;
        if (std::regex_match(tok, m, reRange)) {          // 年レンジ
            int y1 = std::stoi(m[1]), y2 = std::stoi(m[2]);
            if (y1 > y2) std::swap(y1, y2);
            for (auto& kv : horses) {
                int y = horses[kv.first].YearInt;
                if (y >= y1 && y <= y2) targetSet.insert(kv.first);
            }
        }
        else if (std::regex_match(tok, m, reYear)) {    // 単年
            int y = std::stoi(tok);
            for (auto& kv : horses)
                if (horses[kv.first].YearInt == y)
                    targetSet.insert(kv.first);
        }
        else {                                          // PrimaryKey
            if (!horses.count(tok))
                std::cerr << "PrimaryKey \"" << tok << "\" not found - skip\n";
            else
                targetSet.insert(tok);
        }
    }
	std::cout << "[main] " << targetSet.size() << " targets found\n";

    if (targetSet.empty()) { std::cerr << "対象馬が 0 頭でした。\n"; return 1; }

    std::vector<std::string> targetPks(targetSet.begin(), targetSet.end());
    std::sort(targetPks.begin(), targetPks.end());

    /* ---------- 2. idLabel を生成 ---------- */
    std::string idLabel;
    for (size_t i = 0; i < idTokens.size(); ++i) {
        if (i) idLabel += "_";
        idLabel += idTokens[i];
    }
    std::replace_if(idLabel.begin(), idLabel.end(),
        [](char c) { return !std::isalnum((unsigned char)c); }, '_');

    /* ---------- 3. 以降は従来ロジック（targetPks を使う） ---------- */
    // ... allKeys 作成、setAnc / setDesc 収集、CSV 出力など

    // --- 全馬キー (年代順) ---
    std::vector<std::string> allKeys;
    allKeys.reserve(horses.size());
    for (auto& kv : horses) allKeys.push_back(kv.first);
    std::sort(allKeys.begin(), allKeys.end(),
        [&](const std::string& a, const std::string& b) {
            int ya = horses[a].YearInt, yb = horses[b].YearInt;
            return (ya == yb) ? a < b : ya < yb;
        });

    // --- 祖先・子孫セット（targets 全体の和集合） ---
    std::unordered_set<std::string> setAnc, setDesc;
    for (const auto& pk : targetPks) {
        collectAncestors(pk, setAnc);
        collectDescendants(pk, setDesc);
    }

    std::replace(idLabel.begin(), idLabel.end(), ' ', '_'); // 空白→_

    // ==========================================================
    // File-A  行 = 全馬, 列 = targets
    // ==========================================================
    std::string fileA = "blood_of_" + idLabel + "_in_all_horses.csv";
    // ==========================================================
    // File-B  行 = targets, 列 = 全馬
    // ==========================================================
    std::string fileB = "blood_of_all_horses_in_" + idLabel + ".csv";

    // ==========================================================
    // File-A  行 = 全馬, 列 = targets
    // ==========================================================
    if (targetPks.size() == 1) {
        // 進捗ログ付き・列 1 本
        saveDescFast(fileA, allKeys, setDesc, targetPks[0]);
    }
    else {
        // 複数列は汎用関数で
        saveCSVMatrix_Smart(fileA,
            allKeys, targetPks,      // rows, cols
            false,                   // transpose
            [&](auto row, auto) { return setDesc.count(row); });

    }
    std::cout << "[done] " << fileA << '\n';
    dp.clear();

    // ==========================================================
    // File-B  行 = targets, 列 = 全馬
    // ==========================================================
    if (targetPks.size() == 1) {
        saveAncVert(fileB, allKeys, setAnc, targetPks[0]);
    }
    else {
        saveCSVMatrix_Smart(fileB,
            targetPks, allKeys,      // (行←target, 列←全馬) + transpose=true
            true,
            [&](auto row, auto col) { return setAnc.count(col); });

    }
    std::cout << "[done] " << fileB << '\n';

    // --- 終了処理 ---
    db->Flush(rocksdb::FlushOptions());
    db.reset();
    std::cout << "[main] すべて完了しました。\n";
    return 0;
}

