#include "rm_test_util.h"

int main()
{

  bool passedAll = true;
  // By executing this script, the following tables including the system tables will be removed.
  cout << endl << "***** RM TEST - Deleting the Catalog and user tables *****" << endl;

  RC rc = rm->deleteTable("tbl_employee");
  if (rc != 0) {
	  cout << "Deleting tbl_employee failed." << endl;
      passedAll = false;
  }

  rc = rm->deleteTable("tbl_employee2");
  if (rc != 0) {
	  cout << "Deleting tbl_employee2 failed." << endl;
      passedAll = false;
  }

  rc = rm->deleteTable("tbl_employee3");
  if (rc != 0) {
	  cout << "Deleting tbl_employee3 failed." << endl;
      passedAll = false;
  }

  rc = rm->deleteTable("tbl_employee4");
  if (rc != 0) {
	  cout << "Deleting tbl_employee4 failed." << endl;
      passedAll = false;
  }

  rc = rm->deleteCatalog();
  if (rc != 0) {
	  cout << "Deleting the catalog failed." << endl;
      passedAll = false;
  }

  string statusString = passedAll ? "PASSED" : "FAILED";
  cout << endl << "[" << statusString << "] RM TEST - Deleting the Catalog and user tables" << endl;
  return success;
}
