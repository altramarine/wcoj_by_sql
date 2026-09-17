#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>
#include <omp.h>

// Binary input: uint64 edge count, uint64 vertex slots, uint32 endpoint pairs.
int main(int argc, char** argv) {
    if (argc != 5) return 2;
    std::ifstream input(argv[1], std::ios::binary);
    uint64_t n=0, v=0;
    input.read(reinterpret_cast<char*>(&n), 8);
    input.read(reinterpret_cast<char*>(&v), 8);
    std::vector<uint32_t> edges(2*n);
    input.read(reinterpret_cast<char*>(edges.data()), edges.size()*4);
    if (!input || !n || !v) return 3;
    int m=std::stoi(argv[2]), threads=std::stoi(argv[3]);
    uint64_t seed=std::stoull(argv[4]);
    if(m<=0 || threads<=0) return 4;
    std::vector<double> degree(v, 0), f(v);
    for(uint64_t e=0;e<n;++e) degree[edges[2*e]]++;
    for(uint64_t a=0;a<v;++a) f[a]=std::sqrt(degree[a]);
    double exact=0, split=0;
    for(uint64_t e=0;e<n;++e) {
        auto a=edges[2*e], b=edges[2*e+1];
        exact+=f[a]*f[b]; split+=std::min(degree[a],degree[b]);
    }
    std::cout<<std::setprecision(17)<<"n\t"<<n<<"\nm\t"<<m
             <<"\nseed\t"<<seed<<"\nthreads\t"<<threads
             <<"\nexact_K\t"<<exact<<"\nexact_split\t"<<split<<std::endl;
    std::vector<double> products(m), As(m), Bs(m), Cs(m);
    auto start=std::chrono::steady_clock::now();
    // Each repetition has its own PRNG stream. Two separate draws per key
    // give independent sign families for A and B even for this self join.
    #pragma omp parallel for num_threads(threads) schedule(dynamic)
    for(int j=0;j<m;++j) {
        std::seed_seq seq{uint32_t(seed), uint32_t(seed>>32), uint32_t(j)};
        std::mt19937_64 rng(seq);
        std::vector<int8_t> sigma(v), tau(v);
        double B=0,C=0;
        for(uint64_t base=0;base<v;base+=64) {
            uint64_t s=rng(), t=rng();
            for(uint64_t k=0;k<std::min(uint64_t(64),v-base);++k) {
                int a=2*int((s>>k)&1)-1, b=2*int((t>>k)&1)-1;
                sigma[base+k]=a; tau[base+k]=b;
                B+=f[base+k]*a; C+=f[base+k]*b;
            }
        }
        int64_t A=0;
        for(uint64_t e=0;e<n;++e) A+=int(sigma[edges[2*e]])*int(tau[edges[2*e+1]]);
        As[j]=double(A); Bs[j]=B; Cs[j]=C;
        products[j]=double(A)*B*C;
    }
    double elapsed=std::chrono::duration<double>(std::chrono::steady_clock::now()-start).count();
    auto reduce_start=std::chrono::steady_clock::now();
    long double sum=0, ss=0;
    for(auto z:products) sum+=z;
    double estimate=double(sum/m);
    for(auto z:products) ss+=(z-estimate)*(z-estimate);
    double se=std::sqrt(double(ss/(m-1)/m));
    double reduction=std::chrono::duration<double>(std::chrono::steady_clock::now()-reduce_start).count();
    std::cout<<"build_seconds\t"<<elapsed<<"\nreduce_seconds\t"<<reduction
             <<"\nestimate_K\t"<<estimate<<"\nrelative_error\t"<<(estimate/exact-1)
             <<"\nempirical_standard_error\t"<<se<<"\nrelative_standard_error\t"<<se/exact
             <<"\nsummary_bytes_three_arrays\t"<<3*m*8<<std::endl;
    std::ofstream samples(std::string(argv[1])+".samples.tsv");
    samples<<std::setprecision(17)<<"j\tA\tB\tC\tproduct\n";
    for(int j=0;j<m;++j) samples<<j<<'\t'<<As[j]<<'\t'<<Bs[j]<<'\t'<<Cs[j]<<'\t'<<products[j]<<'\n';
}
