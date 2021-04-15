#include <iostream>
#include <sys/mman.h>
using namespace std;

int main(){
	int* a = new int[1000];
	int succ = mlock(a, 1000*sizeof(int));
	int x; cin >> x; 
	return 0;
}