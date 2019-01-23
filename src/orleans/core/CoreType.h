#pragma once

#include <string>
#include <cstdint>

namespace orleans { namespace core {
    
    using GrainTypeName = std::string;
    using IPAddr = std::pair<std::string, uint32_t>;

    const std::string OrleansReplyObjKey = "reply";

} }