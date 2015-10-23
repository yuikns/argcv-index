##############################################################################
#
# aschd.cc is a entry of index build & querier
#
##############################################################################

ADD_EXECUTABLE(aschd aschd.cc)

ADD_DEPENDENCIES(aschd argcv)
ADD_DEPENDENCIES(aschd index_msg)



# http://www.gnu.org/software/libc/manual/html_node/Feature-Test-Macros.html#index-_005fPOSIX_005fSOURCE 
#ADD_DEFINITIONS( " -D_POSIX_SOURCE ") 
#INCLUDE(ExternalProject) # IF NEED EXTEND PROJECT
#INCLUDE(ExternalLeveldb)
#MESSAGE(STATUS "$$$$$$$$$$ INCLUDE LEVELDB DIR " ${LEVELDB_INCLUDE_DIR})
#INCLUDE_DIRECTORIES(${LEVELDB_INCLUDE_DIR})

TARGET_LINK_LIBRARIES(aschd argcv_static)
TARGET_LINK_LIBRARIES(aschd argcv_ir_index_static)
TARGET_LINK_LIBRARIES(aschd argcv_string_static)
TARGET_LINK_LIBRARIES(aschd argcv_sys_static)
TARGET_LINK_LIBRARIES(aschd argcv_wrapper_leveldb_static)
TARGET_LINK_LIBRARIES(aschd argcv_wrapper_ini_static)
TARGET_LINK_LIBRARIES(aschd argcv_storage_static)

#TARGET_LINK_LIBRARIES(argcv_seed argcv_wrapper_curl_static)

TARGET_LINK_LIBRARIES(aschd ${CRFPP_LIBRARY_STATIC})
TARGET_LINK_LIBRARIES(aschd ${LEVELDB_LIBRARY_STATIC})
TARGET_LINK_LIBRARIES(aschd ${MONGO_CXX_LIBRARY_STATIC})

FIND_PACKAGE(Protobuf REQUIRED)
TARGET_LINK_LIBRARIES(aschd index_msg ${PROTOBUF_LIBRARIES}) # protobuf


#  executable properties
SET_TARGET_PROPERTIES(aschd PROPERTIES OUTPUT_NAME "aschd")



