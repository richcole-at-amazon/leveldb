#include <iostream>
#include <sstream>
#include <string>
#include <map>
#include <vector>
#include <memory>

#include <math.h>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>

using namespace std;

unsigned int random(unsigned int max) {
  return rand() % max;
}

string newString(int32_t index) {
  const unsigned int len = 1024;
  char bytes[len];
  unsigned int i = 0;
  while (i < 8) {
    bytes[i] = 'a' + ((index >> (4 * i)) & 0xf);
    ++i;
  }
  while (i < sizeof(bytes)) {
    bytes[i] = 'a' + random(26);
    ++i;
  }
  return string(bytes, sizeof(bytes));
}

int main(int argc, char** argv) {
  srandom(0);

  bool delete_before_put = false;
  bool keep_snapshots = true;

  vector<pair<string, string> *> test_map(10000, NULL);
  vector<leveldb::Snapshot const*> snapshots(100, NULL);

  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  int system_status;

  system_status = system("rm -rf ./issue320_test_db");
  leveldb::Status status = leveldb::DB::Open(options, "./issue320_test_db",
      &db);

  if (false == status.ok()) {
    cerr << "Unable to open/create test database './issue320_test_db'" << endl;
    cerr << status.ToString() << endl;
    return -1;
  }

  unsigned int target_size = 10000;
  unsigned int num_items = 0;
  unsigned long count = 0;
  string key;
  string value, old_value;

  leveldb::WriteOptions writeOptions;
  leveldb::ReadOptions readOptions;
  while (count < 200000) {
    if ((++count % 1000) == 0) {
      cout << "count: " << count << endl;
    }

    unsigned int index = random(test_map.size());
    leveldb::WriteBatch batch;

    if (test_map[index] == NULL) {
      num_items++;
      test_map[index] = new pair<string, string>(newString(index),
          newString(index));
      batch.Put(test_map[index]->first, test_map[index]->second);
    } else {
      if (!db->Get(readOptions, test_map[index]->first, &old_value).ok()) {
        cout << "read failed" << endl;
        abort();
      }
      if (old_value != test_map[index]->second) {
        cout << "ERROR incorrect value returned by Get" << endl;
        cout << "  count=" << count << endl;
        cout << "  old value=" << old_value << endl;
        cout << "  test_map[index]->second=" << test_map[index]->second << endl;
        cout << "  test_map[index]->first=" << test_map[index]->first << endl;
        cout << "  index=" << index << endl;
        exit(-1);
      }

      if (num_items >= target_size && random(100) > 30) {
        batch.Delete(test_map[index]->first);
        delete test_map[index];
        test_map[index] = NULL;
        --num_items;
      } else {
        test_map[index]->second = newString(index);
        if (delete_before_put)
          batch.Delete(test_map[index]->first);
        batch.Put(test_map[index]->first, test_map[index]->second);
      }
    }

    if (!db->Write(writeOptions, &batch).ok()) {
      cout << "write failed" << endl;
      abort();
    }

    if (keep_snapshots && random(10) == 0) {
      unsigned int i = random(snapshots.size());
      if (snapshots[i] != NULL) {
        db->ReleaseSnapshot(snapshots[i]);
      }
      snapshots[i] = db->GetSnapshot();
    }
  }

  for (size_t i = 0; i < test_map.size(); ++i) {
    if (test_map[i] != NULL) {
      delete test_map[i];
      test_map[i] = NULL;
    }
  }

  system_status = system("rm -rf ./issue320_test_db");
  return 0;
}
