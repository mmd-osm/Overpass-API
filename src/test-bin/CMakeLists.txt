
function(build_template_test test_name)
  cmake_parse_arguments(test_param "" "" "" "")
  add_executable(${test_name} ${${test_name}_SOURCES} $<TARGET_OBJECTS:types_test> $<TARGET_OBJECTS:compression_test>)
  target_link_libraries(${test_name} ${LIBS})
endfunction()

function(build_stmt_test test_name)
  cmake_parse_arguments(test_param "" "" "" "")
  add_executable(${test_name} ${${test_name}_SOURCES} $<TARGET_OBJECTS:types_test> $<TARGET_OBJECTS:compression_test> $<TARGET_OBJECTS:statements_test> $<TARGET_OBJECTS:testenv_test>)
  target_link_libraries(${test_name} ${LIBS})
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


set(output_SOURCES
  ../overpass_api/frontend/output.cc
)


set(expatwrapper_SOURCES
  ../expat/expat_justparse_interface.cc
)

set(testenv_SOURCES
  ${settings_SOURCES}
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

add_library(statements_test OBJECT ${statements_cc_SOURCES})
add_library(testenv_test OBJECT ${testenv_SOURCES})
add_library(compression_test OBJECT ${compression_SOURCES})
add_library(types_test OBJECT ${types_SOURCES})

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

# Build node/way/relation/diff_updater


