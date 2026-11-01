#include "../research/real_decoders_20261024/src_patch_fusion/fullsql_reference.hpp"

int main(int argc, char** argv) {
    try {
        physical_fill_mode = 3;
        return main3(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
