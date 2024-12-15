cmake_minimum_required(VERSION 3.10)
project(ParquetServer)

############################## Custom build modes ###############################

set(CMAKE_CXX_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
  CACHE STRING
  "Flags used by the C++ compiler during Sanitize builds."
  FORCE)
set(CMAKE_C_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
  CACHE STRING
  "Flags used by the C compiler during Sanitize builds."
  FORCE)
set(CMAKE_EXE_LINKER_FLAGS_SANITIZE
  ${CMAKE_EXE_LINKER_FLAGS_DEBUG} CACHE STRING
  "Flags used for linking binaries during Sanitize builds."
  FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  ${CMAKE_SHARED_LINKER_FLAGS_DEBUG} CACHE STRING
  "Flags used by the shared libraries linker during Sanitize builds."
  FORCE)
mark_as_advanced(
  CMAKE_CXX_FLAGS_SANITIZE		  CMAKE_EXE_LINKER_FLAGS_SANITIZE
  CMAKE_C_FLAGS_SANITIZE		  CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  )

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
  "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
  FORCE)

############################### External Projects ###############################

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE) # make sure all dependent libraries are compiled with that option

include(ExternalProject)

# Set Arrow and Parquet versions
set(ARROW_VERSION 7.0.0)

# Set the install directory for Arrow and Parquet
set(ARROW_INSTALL_DIR ${CMAKE_BINARY_DIR}/arrow-install)

# External project to build both Arrow and Parquet (since they are in the same repo)
ExternalProject_Add(arrow_project
    PREFIX ${CMAKE_BINARY_DIR}/arrow
    GIT_REPOSITORY https://github.com/apache/arrow.git
    GIT_TAG apache-arrow-${ARROW_VERSION}
    SOURCE_SUBDIR cpp
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${ARROW_INSTALL_DIR}
        -DARROW_PARQUET=ON         # Enable Parquet in Arrow build
        -DARROW_CSV=ON             # Enable CSV support
        -DARROW_FILESYSTEM=ON      # Enable filesystem support
        -DARROW_BUILD_SHARED=ON    # Build shared libraries
        -DARROW_BUILD_TESTS=OFF
    BUILD_ALWAYS ON
    INSTALL_COMMAND make install
)

set(CROW_INSTALL_DIR ${CMAKE_BINARY_DIR}/crow-install)
ExternalProject_Add(crow
    PREFIX ${CMAKE_BINARY_DIR}/crow
    GIT_REPOSITORY https://github.com/CrowCpp/Crow.git
    GIT_TAG master
    UPDATE_DISCONNECTED ON
    CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CROW_INSTALL_DIR}
	-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
	-DCROW_BUILD_EXAMPLES=OFF
	-DCROW_BUILD_TESTS=OFF
	-DCROW_ENABLE_COMPRESSION=ON
    BUILD_ALWAYS OFF
    BUILD_IN_SOURCE OFF
    BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --config ${CMAKE_BUILD_TYPE}
    INSTALL_COMMAND ${CMAKE_COMMAND} --install <BINARY_DIR> --prefix ${CROW_INSTALL_DIR}

)
  ########################## Other Options and Library Dependencies ############################

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_package(Boost REQUIRED)
  
#################################### Targets ####################################

if(MSVC)
  # not making any difference on Windows
  # and helps Visual Studio to parse it correctly as a target
  set(LibraryType SHARED)
else()
  set(LibraryType MODULE)
endif(MSVC)

if(MSVC)
  set(pluginInstallDir bin)
else()
  set(pluginInstallDir lib)
endif(MSVC)

set(ImplementationFiles Source/ParquetServer.cpp)

add_executable(ParquetServer ${ImplementationFiles})

set_property(TARGET ParquetServer PROPERTY CXX_STANDARD 20)

target_include_directories(ParquetServer SYSTEM PUBLIC ${ParquetServer_BINARY_DIR}/deps/include ${ARROW_INSTALL_DIR}/include ${Boost_INCLUDE_DIRS} ${CROW_INSTALL_DIR}/include)

target_link_directories(ParquetServer PUBLIC ${ParquetServer_BINARY_DIR}/deps/lib ${ARROW_INSTALL_DIR}/lib)
add_dependencies(ParquetServer arrow_project)


set(PUBLIC_HEADER_LIST
  ${CMAKE_CURRENT_SOURCE_DIR}/Source/ParquetServer.hpp;
  )


list(APPEND AllTargets ParquetServer)

foreach(Target IN LISTS AllTargets)
  if(NOT WIN32)
    target_link_libraries(${Target} dl)
  endif(NOT WIN32)
    target_link_libraries(${Target} Threads::Threads)
    set_target_properties(${Target} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    if(MSVC)
        target_compile_options(${Target} PUBLIC "/Zc:__cplusplus")
        target_compile_options(${Target} PUBLIC "/EHsc")
    endif()
    set_target_properties(${Target} PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
    add_dependencies(${Target} arrow_project)
    target_include_directories(${Target} SYSTEM PUBLIC ${ParquetServer_BINARY_DIR}/deps/include)
    target_include_directories(${Target} SYSTEM PUBLIC ${CMAKE_BINARY_DIR}/deps/include)
    target_link_libraries(${Target} arrow parquet ${Boost_LIBRARIES})
    if(UNIX AND NOT APPLE)
        target_link_libraries(${Target} rt)
    endif()
endforeach()

set_target_properties(ParquetServer PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
set_target_properties(ParquetServer PROPERTIES PUBLIC_HEADER ${PUBLIC_HEADER_LIST})

install(TARGETS ParquetServer RUNTIME DESTINATION bin)
