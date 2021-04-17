#include <iostream>
#include <sys/mman.h>
using namespace std;


int main(){
	int* a = new int[1000000];
	int succ = mlock(a, 1000000*sizeof(int));
	return 0;
}