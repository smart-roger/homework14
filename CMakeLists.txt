cmake_minimum_required(VERSION 3.2)

project(yamr VERSION 0.0.$ENV{TRAVIS_BUILD_NUMBER})
#project(yamr VERSION 0.0)

include_directories(
	include
)

find_package (Threads)

add_executable(yamr
					main.cpp )

set_target_properties(yamr PROPERTIES
	CXX_STANDARD 14
	CXX_STANDARD_REQUIRED ON
	COMPILE_OPTIONS -Wpedantic -Wall -Wextra
)

target_link_libraries (yamr ${CMAKE_THREAD_LIBS_INIT})

install(TARGETS yamr RUNTIME DESTINATION bin)

set(CPACK_GENERATOR DEB)

set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${PROJECT_VERSION_PATCH}")

set(CPACK_PACKAGE_CONTACT __shurik@list.ru)

include(CPack)
