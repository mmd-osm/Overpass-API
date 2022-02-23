
# List of source files in statements_cc_SOURCES not sorted due to Unity Build dependencies
# btw area_query and polygon_query: we need to ensure both files are processed in the same batch

set(statements_cc_SOURCES
  overpass_api/statements/area_query.cc
  overpass_api/statements/polygon_query.cc
  overpass_api/data/bbox_filter.cc
  overpass_api/statements/aggregators.cc
  overpass_api/statements/around.cc
  overpass_api/statements/bbox_query.cc
  overpass_api/statements/binary_operators.cc
  overpass_api/statements/changed.cc
  overpass_api/statements/compare.cc
  overpass_api/statements/complete.cc
  overpass_api/statements/convert.cc
  overpass_api/statements/coord_query.cc
  overpass_api/statements/difference.cc
  overpass_api/statements/evaluator.cc
  overpass_api/statements/explicit_geometry.cc
  overpass_api/statements/filter.cc
  overpass_api/statements/for.cc
  overpass_api/statements/foreach.cc
  overpass_api/statements/geometry_endomorphisms.cc
  overpass_api/statements/id_query.cc
  overpass_api/statements/if.cc
  overpass_api/statements/item.cc
  overpass_api/statements/item_geometry.cc
  overpass_api/statements/localize.cc
  overpass_api/statements/make_area.cc
  overpass_api/statements/make.cc
  overpass_api/statements/map_to_area.cc
  overpass_api/statements/newer.cc
  overpass_api/statements/osm_script.cc
  overpass_api/statements/per_member.cc
  overpass_api/statements/pivot.cc
  overpass_api/statements/print.cc
  overpass_api/statements/query.cc
  overpass_api/statements/recurse.cc
  overpass_api/statements/retro.cc
  overpass_api/statements/runtime_value.cc
  overpass_api/statements/set_list_operators.cc
  overpass_api/statements/set_prop.cc
  overpass_api/statements/statement.cc
  overpass_api/statements/string_endomorphisms.cc
  overpass_api/statements/tag_value.cc
  overpass_api/statements/ternary_operator.cc
  overpass_api/statements/timeline.cc
  overpass_api/statements/unary_functions.cc
  overpass_api/statements/unary_operators.cc
  overpass_api/statements/union.cc
  overpass_api/statements/user.cc
  expat/escape_json.cc
  expat/escape_xml.cc
)

set(output_formats_SOURCES
  overpass_api/output_formats/output_csv.cc
  overpass_api/output_formats/output_csv_factory.cc
  overpass_api/output_formats/output_custom.cc
  overpass_api/output_formats/output_custom_factory.cc
  overpass_api/output_formats/output_json.cc
  overpass_api/output_formats/output_json_factory.cc
  overpass_api/output_formats/output_osmium.cc
  overpass_api/output_formats/output_osmium_factory.cc
  overpass_api/output_formats/output_xml.cc
  overpass_api/output_formats/output_xml_factory.cc
  overpass_api/output_formats/output_popup.cc
  overpass_api/output_formats/output_popup_factory.cc
)

set(core_SOURCES
  overpass_api/frontend/output_handler_parser.cc
  overpass_api/statements/statement_dump.cc
  expat/map_ql_input.cc
)

set(data_SOURCES
  overpass_api/data/collect_members.cc
  overpass_api/data/diff_set.cc
  overpass_api/data/geometry_from_quad_coords.cc
  overpass_api/data/relation_geometry_store.cc
  overpass_api/data/set_comparison.cc
  overpass_api/data/way_geometry_store.cc
)

set(dispatcherclient_SOURCES
  template_db/dispatcher_client.cc
)

set(dispatcher_lib_SOURCES
  overpass_api/dispatch/resource_manager.cc
  overpass_api/osm-backend/area_updater.cc
)

set(expatwrapper_SOURCES
  expat/expat_justparse_interface.cc
)

set(frontend_SOURCES
  overpass_api/frontend/cgi-helper.cc
  overpass_api/frontend/user_interface.cc
)
 
set(output_SOURCES
  overpass_api/frontend/output.cc
)

set(settings_SOURCES
  overpass_api/core/settings.cc
)

set(osm_updater_SOURCES
  overpass_api/osm-backend/meta_updater.cc
  overpass_api/osm-backend/basic_updater.cc
  overpass_api/osm-backend/node_updater.cc
  overpass_api/osm-backend/way_updater.cc
  overpass_api/osm-backend/relation_updater.cc
  overpass_api/osm-backend/area_updater.cc
  overpass_api/osm-backend/osm_updater.cc
  overpass_api/osm-backend/osmium_updater.cc
  overpass_api/core/four_field_index.cc
  overpass_api/core/geometry.cc
  expat/escape_xml.cc
)

set(update_database_SOURCES
  overpass_api/osm-backend/update_database.cc
)

set(update_from_dir_SOURCES
  overpass_api/osm-backend/update_from_dir.cc
)

set(types_SOURCES
  template_db/types.cc
)

set(compression_SOURCES
  template_db/zlib_wrapper.cc
  template_db/lz4_wrapper.cc
)

set(query_common_SOURCES
  overpass_api/frontend/basic_formats.cc
  overpass_api/frontend/output_handler.cc  
  overpass_api/dispatch/dispatcher_stub.cc
  overpass_api/frontend/decode_text.cc
  overpass_api/frontend/map_ql_parser.cc
  overpass_api/frontend/tokenizer_utils.cc  
  overpass_api/frontend/decode_text.cc
  overpass_api/frontend/map_ql_parser.cc
  overpass_api/frontend/tokenizer_utils.cc  
  overpass_api/dispatch/scripting_core.cc
  overpass_api/core/geometry.cc  
  overpass_api/core/four_field_index.cc  
  overpass_api/frontend/web_output.cc
)
 
set(osm3s_query_SOURCES
  overpass_api/frontend/console_output.cc
  overpass_api/dispatch/osm3s_query.cc
  overpass_api/osm-backend/clone_database.cc
)

set(interpreter_SOURCES
  overpass_api/dispatch/web_query.cc
)

set(create_tagged_nodes_SOURCES
  overpass_api/osm-backend/create_tagged_nodes.cc
)  

set(create_node_changepack_SOURCES
  overpass_api/osm-backend/create_node_changepack.cc
)

set(dispatcher_SOURCES
  template_db/dispatcher.cc
  template_db/file_tools.cc
  template_db/transaction_insulator.cc
  overpass_api/dispatch/dispatcher_server.cc
)

set(timestamp_SOURCES
  overpass_api/dispatch/db_timestamp.cc
  overpass_api/frontend/web_output.cc
  overpass_api/frontend/basic_formats.cc
  overpass_api/frontend/decode_text.cc
  expat/escape_xml.cc
)

set(map_demo_SOURCES
  overpass_api/osm-backend/map_demo.cc
  overpass_api/frontend/console_output.cc
)

set(export_tables_SOURCES
  overpass_api/osm-backend/export_tables.cc
)

set(import_tables_SOURCES
  overpass_api/osm-backend/import_tables.cc
)

set(dump_file_SOURCES
  overpass_api/osm-backend/dump_file.cc
)
 
add_library(core STATIC ${core_SOURCES})
add_library(data STATIC ${data_SOURCES})
add_library(dispatch STATIC ${dispatcher_lib_SOURCES} ${dispatcherclient_SOURCES})

add_library(frontend OBJECT ${frontend_SOURCES})
add_library(output OBJECT ${output_SOURCES})
add_library(types OBJECT ${types_SOURCES})

add_library(osm_updater OBJECT ${osm_updater_SOURCES} ${types_SOURCES})
add_library(statements OBJECT ${statements_cc_SOURCES})
add_library(output_formats OBJECT ${output_formats_SOURCES})
add_library(query_common OBJECT ${query_common_SOURCES})
add_library(compression OBJECT ${compression_SOURCES})
add_library(expatwrapper OBJECT ${expatwrapper_SOURCES})
add_library(settings OBJECT ${settings_SOURCES})

# more infos: https://onqtam.com/programming/2019-12-20-pch-unity-cmake-3-16/
if (USE_UNITY_BUILD)
  set_target_properties(core PROPERTIES UNITY_BUILD ON)
  set_target_properties(data PROPERTIES UNITY_BUILD ON)
  set_target_properties(frontend PROPERTIES UNITY_BUILD ON)
  set_target_properties(statements PROPERTIES UNITY_BUILD ON)
#  set_target_properties(statements PROPERTIES UNITY_BUILD_BATCH_SIZE 100)

  set_target_properties(osm_updater PROPERTIES UNITY_BUILD ON)
  set_target_properties(output_formats PROPERTIES UNITY_BUILD ON)
#  set_target_properties(output_formats PROPERTIES UNITY_BUILD_BATCH_SIZE 100)
  set_target_properties(query_common PROPERTIES UNITY_BUILD ON)
endif()
 
target_link_libraries(dispatch ${LIBRT})
target_link_libraries(core dispatch)

add_executable(uncgi pt_diagrams/uncgi.cc)
add_executable(tocgi pt_diagrams/tocgi.cc)
add_executable(escape_xml expat/escape_xml.cc expat/escape_xml_main.cc)

if (HAVE_OPENMP)
  set(OPENMP_TARGET_LINK PRIVATE OpenMP::OpenMP_CXX)
endif()  

add_executable(update_database ${update_database_SOURCES} $<TARGET_OBJECTS:osm_updater> $<TARGET_OBJECTS:compression> $<TARGET_OBJECTS:expatwrapper> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:output>)
target_link_libraries(update_database PUBLIC data dispatch ${LIBS} ${OPENMP_TARGET_LINK})

add_executable(update_from_dir ${update_from_dir_SOURCES} $<TARGET_OBJECTS:osm_updater> $<TARGET_OBJECTS:compression> $<TARGET_OBJECTS:expatwrapper> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:output>)
target_link_libraries(update_from_dir PUBLIC data dispatch ${LIBS} ${OPENMP_TARGET_LINK})

add_executable(osm3s_query ${osm3s_query_SOURCES} $<TARGET_OBJECTS:types> $<TARGET_OBJECTS:query_common> $<TARGET_OBJECTS:compression> $<TARGET_OBJECTS:output_formats> $<TARGET_OBJECTS:statements> $<TARGET_OBJECTS:expatwrapper> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:frontend>) 
target_link_libraries(osm3s_query core data ${LIBS})

add_executable(interpreter ${interpreter_SOURCES} $<TARGET_OBJECTS:types> $<TARGET_OBJECTS:query_common> $<TARGET_OBJECTS:compression> $<TARGET_OBJECTS:output_formats> $<TARGET_OBJECTS:statements> $<TARGET_OBJECTS:expatwrapper> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:frontend>)
target_link_libraries(interpreter core data ${LIBS})

add_executable(dispatcher ${dispatcher_SOURCES} $<TARGET_OBJECTS:types> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:frontend>)
target_link_libraries(dispatcher dispatch ${LIBS})

add_executable(create_tagged_nodes ${create_tagged_nodes_SOURCES} $<TARGET_OBJECTS:types> $<TARGET_OBJECTS:compression> $<TARGET_OBJECTS:expatwrapper> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:frontend>)
target_link_libraries(create_tagged_nodes core data ${LIBS})

add_executable(create_node_changepack ${create_node_changepack_SOURCES} $<TARGET_OBJECTS:types> $<TARGET_OBJECTS:compression> $<TARGET_OBJECTS:expatwrapper> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:frontend>)
target_link_libraries(create_node_changepack core data ${LIBS})

add_executable(timestamp ${timestamp_SOURCES} $<TARGET_OBJECTS:types> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:compression>)
target_link_libraries(timestamp dispatch ${LIBS})

add_executable(map_demo ${map_demo_SOURCES} $<TARGET_OBJECTS:types> $<TARGET_OBJECTS:query_common> $<TARGET_OBJECTS:compression> $<TARGET_OBJECTS:output_formats> $<TARGET_OBJECTS:statements> $<TARGET_OBJECTS:expatwrapper> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:frontend>)
target_link_libraries(map_demo core data ${LIBS})

add_executable(export_tables ${export_tables_SOURCES} $<TARGET_OBJECTS:compression> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:types>)
target_link_libraries(export_tables data ${LIBS})

add_executable(import_tables ${import_tables_SOURCES} $<TARGET_OBJECTS:compression> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:types>)
target_link_libraries(import_tables data ${LIBS})

add_executable(dump_file ${dump_file_SOURCES} $<TARGET_OBJECTS:compression> $<TARGET_OBJECTS:settings> $<TARGET_OBJECTS:types>)
target_link_libraries(dump_file core data ${LIBS})
