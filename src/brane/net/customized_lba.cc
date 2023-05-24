#include <brane/net/customized_lba.hh>

namespace brane {

std::unique_ptr<lba_policy> customized_lba::impl_{nullptr};

} // namespace brane
