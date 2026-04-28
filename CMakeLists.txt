cmake_minimum_required (VERSION 3.11)
project (sisl CXX)

# Set Global CMake Options
set_property(GLOBAL PROPERTY USE_FOLDERS ON) # turn on folder hierarchies

include (cmake/Flags.cmake)

set(CMAKE_CXX_STANDARD 23)

enable_testing()
find_package(GTest QUIET REQUIRED)

if (DEFINED BUILD_COVERAGE)
  if (${BUILD_COVERAGE})
    include (cmake/CodeCoverage.cmake)
    APPEND_COVERAGE_COMPILER_FLAGS()
    SETUP_TARGET_FOR_COVERAGE_GCOVR_XML(NAME coverage EXECUTABLE ctest DEPENDENCIES )
  endif()
endif()

if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    message(STATUS "Debug build")
    add_flags("-D_DEBUG")
endif()

if (DEFINED MALLOC_IMPL)
    if (${MALLOC_IMPL} STREQUAL "jemalloc")
        add_flags("-DUSE_JEMALLOC=1")
        if (DEFINED ADDRESS_SANITIZER_ON)
            unset(ADDRESS_SANITIZER_ON CACHE)
        endif()
        if (DEFINED THREAD_SANITIZER_ON)
            unset(THREAD_SANITIZER_ON CACHE)
        endif()
    elseif (${MALLOC_IMPL} STREQUAL "tcmalloc")
        add_flags("-DUSING_TCMALLOC=1")
    endif()
endif()

if ((DEFINED THREAD_SANITIZER_ON) AND (${THREAD_SANITIZER_ON}))
    set(SANITIZER_TYPE "thread")
    include(cmake/sanitize.cmake)
elseif ((DEFINED ADDRESS_SANITIZER_ON) AND (${ADDRESS_SANITIZER_ON}))
    set(SANITIZER_TYPE "address")
    include(cmake/sanitize.cmake)
endif()

include(cmake/settings_gen.cmake)

if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL "Windows") 
    set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
endif()

find_program(CCACHE_FOUND ccache)
if (CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif ()

if((${CMAKE_CXX_COMPILER_ID} STREQUAL "GNU") OR (${CMAKE_CXX_COMPILER_ID} STREQUAL "Clang"))
    add_flags("-Wall -Wextra -Werror -Wpedantic")
endif()

if(UNIX)
    # enable proper GNU extensions
    add_flags("-D_POSIX_C_SOURCE=200809L -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE")
    if(APPLE)
        # _POSIX_C_SOURCE hides Darwin extensions (ipv6_mreq, NI_MAXHOST, etc.); restore them
        add_flags("-D_DARWIN_C_SOURCE")
        # Conan-generated cmake configs expand transitive absolute-path libraries at each step,
        # so the same .a can appear more than once on the link line; ld ignores extras safely.
        string(APPEND CMAKE_EXE_LINKER_FLAGS " -Wl,-no_warn_duplicate_libraries")
        string(APPEND CMAKE_SHARED_LINKER_FLAGS " -Wl,-no_warn_duplicate_libraries")
    endif()
endif()

include_directories(BEFORE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# stdexec's internal rapids-cmake/CPM uses FetchContent_Populate directly; suppress the CMP0169 warning
if(POLICY CMP0169)
    cmake_policy(SET CMP0169 OLD)
endif()

include(FetchContent)
FetchContent_Declare(
    stdexec
    GIT_REPOSITORY https://github.com/NVIDIA/stdexec.git
    GIT_TAG        nvhpc-25.09
    GIT_SHALLOW    TRUE
)
set(STDEXEC_BUILD_TESTS      OFF CACHE BOOL "" FORCE)
set(STDEXEC_BUILD_EXAMPLES   OFF CACHE BOOL "" FORCE)
set(STDEXEC_ENABLE_CUDA      OFF CACHE BOOL "" FORCE)
set(STDEXEC_ENABLE_TBB       OFF CACHE BOOL "" FORCE)
set(STDEXEC_ENABLE_IO_URING  OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(stdexec)

# GCC fires -Wempty-body on macro-generated else-clauses in stdexec's __tuple.hpp.
# The header is transitively included by every TU that pulls in stdexec/execution.hpp,
# so suppress on the INTERFACE target to propagate to all consumers, and on
# system_context for the compiled translation unit.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(stdexec INTERFACE -Wno-empty-body)
    target_compile_options(system_context PRIVATE -Wno-empty-body)
endif()

add_subdirectory(src)

# build info
string(TOUPPER "${CMAKE_BUILD_TYPE}" UC_CMAKE_BUILD_TYPE)
message(STATUS "Build configuration:       ${CMAKE_BUILD_TYPE}")
message(STATUS "C compiler info:           ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION} ${CMAKE_C_COMPILER_EXTERNAL_TOOLCHAIN}")
message(STATUS "C++ compiler info:         ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION} ${CMAKE_CXX_COMPILER_EXTERNAL_TOOLCHAIN}")
message(STATUS "C flags:                   ${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
message(STATUS "C++ flags:                 ${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
message(STATUS "Linker flags (executable): ${CMAKE_EXE_LINKER_FLAGS} ${CMAKE_EXE_LINKER_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
message(STATUS "Linker flags (shared):     ${CMAKE_SHARED_LINKER_FLAGS} ${CMAKE_SHARED_LINKER_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
message(STATUS "Linker flags (module):     ${CMAKE_MODULE_LINKER_FLAGS} ${CMAKE_MODULE_LINKER_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
message(STATUS "Linker flags (static):     ${CMAKE_STATIC_LINKER_FLAGS} ${CMAKE_STATIC_LINKER_FLAGS_${UC_CMAKE_BUILD_TYPE}}")
