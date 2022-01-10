
if (HAVE_OPENMP)
  set(OPENMP_TARGET_LINK PRIVATE OpenMP::OpenMP_CXX)
endif()  

function(build_template_test test_name)
  cmake_parse_arguments(test_param "" "" "" "")
  add_executable(${test_name} ${${test_name}_SOURCES} $<TARGET_OBJECTS:types_test> $<TARGET_OBJECTS:compression_test>)
  target_link_libraries(${test_name} PUBLIC ${LIBS})
endfunction()

function(build_stmt_test test_name)
  cmake_parse_arguments(test_param "" "" "" "")
  add_executable(${test_name} ${${test_name}_SOURCES} $<TARGET_OBJECTS:types_test> $<TARGET_OBJECTS:compression_test>
                             $<TARGET_OBJECTS:statements_test> $<TARGET_OBJECTS:testenv_test>)
  target_link_libraries(${test_name} PUBLIC ${LIBS})
endfunction()

function(build_updater_test test_name)
  cmake_parse_arguments(test_param "" "" "" "")
  add_executable(${test_name} ${${test_name}_SOURCES} $<TARGET_OBJECTS:testenv_test>
                              $<TARGET_OBJECTS:types_test> $<TARGET_OBJECTS:compression_test> 
                              $<TARGET_OBJECTS:updater_common>
                              $<TARGET_OBJECTS:statements_test>
                              )
  target_link_libraries(${test_name} PUBLIC ${LIBS} ${OPENMP_TARGET_LINK})
endfunction()

set(statements_cc_SOURCES
  ../expat/escape_json.cc
  ../expat/escape_xml.cc
  ../overpass_api/core/four_field_index.cc
  ../overpass_api/core/geometry.cc
  ../overpass_api/data/bbox_filter.cc
  ../overpass_api/data/collect_members.cc
  ../overpass_api/data/diff_set.cc
  ../overpass_api/data/geometry_from_quad_coords.cc
  ../overpass_api/data/relation_geometry_store.cc
  ../overpass_api/data/set_comparison.cc
  ../overpass_api/data/way_geometry_store.cc
  ../overpass_api/frontend/output_handler_parser.cc
  ../overpass_api/osm-backend/area_updater.cc
  ../overpass_api/statements/aggregators.cc
  ../overpass_api/statements/area_query.cc
  ../overpass_api/statements/around.cc
  ../overpass_api/statements/bbox_query.cc
  ../overpass_api/statements/binary_operators.cc
  ../overpass_api/statements/changed.cc
  ../overpass_api/statements/compare.cc
  ../overpass_api/statements/complete.cc
  ../overpass_api/statements/convert.cc
  ../overpass_api/statements/coord_query.cc
  ../overpass_api/statements/difference.cc
  ../overpass_api/statements/evaluator.cc
  ../overpass_api/statements/explicit_geometry.cc
  ../overpass_api/statements/filter.cc
  ../overpass_api/statements/for.cc
  ../overpass_api/statements/foreach.cc
  ../overpass_api/statements/geometry_endomorphisms.cc
  ../overpass_api/statements/id_query.cc
  ../overpass_api/statements/if.cc
  ../overpass_api/statements/item.cc
  ../overpass_api/statements/item_geometry.cc
  ../overpass_api/statements/localize.cc
  ../overpass_api/statements/make_area.cc
  ../overpass_api/statements/make.cc
  ../overpass_api/statements/map_to_area.cc
  ../overpass_api/statements/newer.cc
  ../overpass_api/statements/osm_script.cc
  ../overpass_api/statements/per_member.cc
  ../overpass_api/statements/pivot.cc
  ../overpass_api/statements/polygon_query.cc
  ../overpass_api/statements/print.cc
  ../overpass_api/statements/query.cc
  ../overpass_api/statements/recurse.cc
  ../overpass_api/statements/retro.cc
  ../overpass_api/statements/runtime_value.cc
  ../overpass_api/statements/set_list_operators.cc
  ../overpass_api/statements/set_prop.cc
  ../overpass_api/statements/statement.cc
  ../overpass_api/statements/string_endomorphisms.cc
  ../overpass_api/statements/tag_value.cc
  ../overpass_api/statements/ternary_operator.cc
  ../overpass_api/statements/testing_tools.cc
  ../overpass_api/statements/timeline.cc
  ../overpass_api/statements/unary_functions.cc
  ../overpass_api/statements/unary_operators.cc
  ../overpass_api/statements/union.cc
  ../overpass_api/statements/user.cc
)

set(settings_SOURCES
  ../overpass_api/core/settings.cc
)

set(testenv_SOURCES
  ../overpass_api/core/settings.cc
  ../overpass_api/dispatch/resource_manager.cc
  ../overpass_api/frontend/console_output.cc
  ../overpass_api/frontend/user_interface.cc
  ../overpass_api/frontend/output.cc
  ../overpass_api/frontend/basic_formats.cc
  ../overpass_api/frontend/cgi-helper.cc
  ../overpass_api/frontend/decode_text.cc
  ../overpass_api/frontend/output_handler.cc
  ../overpass_api/frontend/tokenizer_utils.cc
  ../expat/map_ql_input.cc
  ../overpass_api/output_formats/output_xml.cc
  ../overpass_api/output_formats/output_xml_factory.cc
)

set(types_SOURCES
  ../template_db/types.cc
)

set(compression_SOURCES
  ../template_db/zlib_wrapper.cc
  ../template_db/lz4_wrapper.cc
)

set(file_blocks_SOURCES
  ../template_db/file_blocks.test.cc
 )

set(block_backend_SOURCES
  ../template_db/block_backend.test.cc
)

set(random_file_SOURCES
  ../template_db/random_file.test.cc
)

set(test_dispatcher_SOURCES
  ../template_db/dispatcher.test.cc
  ../template_db/dispatcher_client.cc
  ../template_db/dispatcher.cc
  ../template_db/file_tools.cc
  ../template_db/transaction_insulator.cc
)

set(area_query_SOURCES
   ../overpass_api/statements/area_query.test.cc
)

set(around_SOURCES
  ../overpass_api/statements/around.test.cc
)

set(bbox_query_SOURCES
  ../overpass_api/statements/bbox_query.test.cc
)

set(complete_SOURCES
  ../overpass_api/statements/complete.test.cc
)

set(convert_SOURCES
  ../overpass_api/statements/convert.test.cc
)

set(difference_SOURCES
  ../overpass_api/statements/difference.test.cc
)

set(foreach_SOURCES
  ../overpass_api/statements/foreach.test.cc
)

set(if_SOURCES
  ../overpass_api/statements/if.test.cc
)

set(make_SOURCES
  ../overpass_api/statements/make.test.cc
)

set(make_area_SOURCES
  ../overpass_api/statements/make_area.test.cc
)

set(polygon_query_SOURCES
  ../overpass_api/statements/polygon_query.test.cc
)

set(print_SOURCES
  ../overpass_api/statements/print.test.cc
)

set(query_SOURCES
  ../overpass_api/statements/query.test.cc
)

set(recurse_SOURCES
  ../overpass_api/statements/recurse.test.cc
)

set(union_SOURCES
  ../overpass_api/statements/union.test.cc
)

set(make_area_SOURCES
  ../overpass_api/statements/make_area.test.cc
)

set(updater_common_SOURCES
  ../overpass_api/osm-backend/meta_updater.cc
  ../overpass_api/osm-backend/basic_updater.cc
  ../overpass_api/osm-backend/node_updater.cc
  ../overpass_api/osm-backend/way_updater.cc
  ../overpass_api/osm-backend/relation_updater.cc
  ../expat/expat_justparse_interface.cc
)

set(node_updater_SOURCES
  ../overpass_api/osm-backend/node_updater.test.cc
)

set(way_updater_SOURCES
  ../overpass_api/osm-backend/way_updater.test.cc
)

set(relation_updater_SOURCES
  ../overpass_api/osm-backend/relation_updater.test.cc
)

set(diff_updater_SOURCES 
  ../overpass_api/osm-backend/diff_updater.test.cc
)

set(compare_osm_base_maps_SOURCES 
  ../overpass_api/osm-backend/compare_osm_base_maps.test.cc
)

set(dump_database_SOURCES 
  ../overpass_api/osm-backend/dump_database.test.cc
)

set(consistency_check_SOURCES 
  ../overpass_api/dispatch/consistency_check.cc 
  ../overpass_api/dispatch/scripting_core.cc
  ../overpass_api/dispatch/dispatcher_stub.cc
  ../overpass_api/frontend/map_ql_parser.cc
  ../overpass_api/statements/statement_dump.cc
  ../template_db/dispatcher_client.cc
)

set(index_computations_SOURCES
  ../overpass_api/core/index_computations.test.cc
)

set(four_field_index_SOURCES
  ../overpass_api/core/four_field_index.cc
  ../overpass_api/core/four_field_index.test.cc
)

set(settings_SOURCES
  ../overpass_api/core/settings.cc
)  

add_library(statements_test OBJECT ${statements_cc_SOURCES})
add_library(testenv_test OBJECT ${testenv_SOURCES})
add_library(compression_test OBJECT ${compression_SOURCES})
add_library(types_test OBJECT ${types_SOURCES})
add_library(updater_common OBJECT ${updater_common_SOURCES})
add_library(settings_test OBJECT ${settings_SOURCES})

if (USE_UNITY_BUILD)
  set_target_properties(statements_test PROPERTIES UNITY_BUILD ON)
endif()

# Build template test binaries
build_template_test(file_blocks)
build_template_test(block_backend)
build_template_test(random_file)
build_template_test(test_dispatcher)

# Build statement test binaries
build_stmt_test(area_query)
build_stmt_test(around)
build_stmt_test(bbox_query)
build_stmt_test(complete)
build_stmt_test(convert)
build_stmt_test(difference)
build_stmt_test(foreach)
build_stmt_test(if)
build_stmt_test(make)
build_stmt_test(make_area)
build_stmt_test(polygon_query)
build_stmt_test(print)
build_stmt_test(query)
build_stmt_test(recurse)
build_stmt_test(union)

# Build node/way/relation_updater
build_updater_test(node_updater)
build_updater_test(way_updater)
build_updater_test(relation_updater)

add_executable(diff_updater ${diff_updater_SOURCES} ${settings_SOURCES} $<TARGET_OBJECTS:types_test>  $<TARGET_OBJECTS:compression_test>)
target_link_libraries(diff_updater ${LIBS})

add_executable(compare_osm_base_maps ${compare_osm_base_maps_SOURCES} $<TARGET_OBJECTS:types_test> $<TARGET_OBJECTS:compression_test> $<TARGET_OBJECTS:settings_test>)
target_link_libraries(compare_osm_base_maps ${LIBS})

add_executable(consistency_check ${consistency_check_SOURCES}
                                 $<TARGET_OBJECTS:statements_test> $<TARGET_OBJECTS:types_test>
                                 $<TARGET_OBJECTS:testenv_test> $<TARGET_OBJECTS:compression_test>)
target_link_libraries(consistency_check ${LIBS})

add_executable(index_computations ${index_computations_SOURCES})
add_executable(four_field_index ${four_field_index_SOURCES})
add_executable(generate_test_file ../overpass_api/osm-backend/generate_test_file.cc)
add_executable(generate_test_file_areas ../overpass_api/osm-backend/generate_test_file_areas.cc)
add_executable(generate_test_file_interpreter ../overpass_api/osm-backend/generate_test_file_interpreter.cc $<TARGET_OBJECTS:settings_test>)
add_executable(generate_test_file_meta ../overpass_api/osm-backend/generate_test_file_meta.cc $<TARGET_OBJECTS:settings_test>)