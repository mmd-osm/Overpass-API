

function(set_test test_name)
    cmake_parse_arguments(test_param "" "" "LABELS" ${ARGN})

    add_executable(${test_name} ${${test_name}_SOURCES})
    target_link_libraries(${test_name} ${LIBS})
    add_test(NAME ${test_name} COMMAND ${test_name})

    set_tests_properties(${test_name} PROPERTIES ENVIRONMENT SRCPATH=${CMAKE_CURRENT_SOURCE_DIR})

    message(STATUS "Added test: ${test_name}...")
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
  ../template_db/lz4_wrapper.cc
  ../template_db/types.cc
  ../template_db/zlib_wrapper.cc
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

set(file_blocks_SOURCES
  ../template_db/file_blocks.test.cc
  ../template_db/types.cc
  ../template_db/zlib_wrapper.cc
  ../template_db/lz4_wrapper.cc
)

set(block_backend_SOURCES
  ../template_db/block_backend.test.cc
  ../template_db/types.cc
  ../template_db/zlib_wrapper.cc
  ../template_db/lz4_wrapper.cc
)

set(random_file_SOURCES
  ../template_db/random_file.test.cc
  ../template_db/types.cc
  ../template_db/zlib_wrapper.cc
  ../template_db/lz4_wrapper.cc
)



# Define test cases

set_test(file_blocks LABELS group_file)
set_test(block_backend LABELS group_file)
set_test(random_file LABELS group_file)




