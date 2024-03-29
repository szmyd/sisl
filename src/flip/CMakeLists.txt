cmake_minimum_required(VERSION 3.11)

add_subdirectory (proto)

if(NOT ${CMAKE_CURRENT_BINARY_DIR} STREQUAL ${CMAKE_CURRENT_SOURCE_DIR})
  include_directories(BEFORE ${CMAKE_CURRENT_BINARY_DIR})
endif()
include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR})

add_library(sisl_flip)
target_sources(sisl_flip PRIVATE
          lib/flip_rpc_server.cpp
          $<TARGET_OBJECTS:flip_proto>
        )
target_link_libraries(sisl_flip PUBLIC
          sisl_logging
          gRPC::grpc++
          spdlog::spdlog
          nlohmann_json::nlohmann_json
        )

if (DEFINED ENABLE_TESTING)
  if (${ENABLE_TESTING})
    add_executable(test_flip)
    target_sources(test_flip PRIVATE
        lib/test_flip.cpp
      )
    target_link_libraries(test_flip sisl_flip cxxopts::cxxopts)
    add_test(NAME Flip COMMAND test_flip)

    add_executable(test_flip_server)
    target_sources(test_flip_server PRIVATE
        lib/test_flip_server.cpp
      )
    target_link_libraries(test_flip_server sisl_flip cxxopts::cxxopts)

    add_executable(test_flip_local_client)
    target_sources(test_flip_local_client PRIVATE
        client/local/test_flip_local_client.cpp
      )
    target_link_libraries(test_flip_local_client sisl_flip cxxopts::cxxopts)
    add_test(NAME FlipLocalClient COMMAND test_flip_local_client)
  endif()
endif()
