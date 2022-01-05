


set(statements_cc_SOURCES
  overpass_api/data/bbox_filter.cc
  overpass_api/statements/aggregators.cc
  overpass_api/statements/area_query.cc
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
  overpass_api/statements/polygon_query.cc
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
  template_db/types.cc
  template_db/zlib_wrapper.cc
  template_db/lz4_wrapper.cc  
)

set(update_database_SOURCES
  overpass_api/osm-backend/update_database.cc
)

set(update_from_dir_SOURCES
  overpass_api/osm-backend/update_from_dir.cc
)

 
set(osm3s_query_SOURCES
  overpass_api/frontend/basic_formats.cc
  overpass_api/frontend/output_handler.cc  
  overpass_api/dispatch/dispatcher_stub.cc
  template_db/types.cc
  overpass_api/frontend/decode_text.cc
  overpass_api/frontend/map_ql_parser.cc
  overpass_api/frontend/tokenizer_utils.cc  
  template_db/zlib_wrapper.cc
  template_db/lz4_wrapper.cc
  overpass_api/dispatch/scripting_core.cc
  overpass_api/core/geometry.cc  
  overpass_api/core/four_field_index.cc  
  overpass_api/frontend/console_output.cc
  overpass_api/frontend/web_output.cc
  overpass_api/dispatch/osm3s_query.cc
  overpass_api/osm-backend/clone_database.cc
)

set(interpreter_SOURCES
  overpass_api/frontend/basic_formats.cc
  overpass_api/frontend/output_handler.cc  
  overpass_api/dispatch/dispatcher_stub.cc
  template_db/types.cc
  overpass_api/frontend/decode_text.cc
  overpass_api/frontend/map_ql_parser.cc
  overpass_api/frontend/tokenizer_utils.cc  
  template_db/zlib_wrapper.cc
  template_db/lz4_wrapper.cc
  overpass_api/dispatch/scripting_core.cc
  overpass_api/core/geometry.cc  
  overpass_api/core/four_field_index.cc  
  overpass_api/dispatch/web_query.cc
  overpass_api/frontend/web_output.cc
)

set(tagged_nodes_SOURCES
  overpass_api/osm-backend/create_tagged_nodes.cc
  template_db/types.cc
  template_db/zlib_wrapper.cc
  template_db/lz4_wrapper.cc
)  

set(dispatcher_SOURCES
  template_db/dispatcher.cc
  template_db/file_tools.cc
  template_db/transaction_insulator.cc
  template_db/types.cc
  overpass_api/dispatch/dispatcher_server.cc
)
 
add_library(core_lib STATIC ${core_SOURCES})
add_library(data_lib STATIC ${data_SOURCES})
add_library(dispatcherclient_lib STATIC ${dispatcherclient_SOURCES})
add_library(dispatcher_lib STATIC ${dispatcher_lib_SOURCES})
add_library(expatwrapper_lib STATIC ${expatwrapper_SOURCES})
add_library(frontend_lib STATIC ${frontend_SOURCES})
add_library(output_lib STATIC ${output_SOURCES})
add_library(settings_lib STATIC ${settings_SOURCES})
add_library(osm_updater_lib STATIC ${osm_updater_SOURCES})
 
target_link_libraries(dispatcherclient_lib ${LIBRT})
target_link_libraries(dispatcher_lib dispatcherclient_lib)
target_link_libraries(core_lib dispatcher_lib expatwrapper_lib settings_lib frontend_lib)
target_link_libraries(frontend_lib output_lib)
target_link_libraries(expatwrapper_lib ${EXPAT})

add_executable(uncgi pt_diagrams/uncgi.cc)
add_executable(tocgi pt_diagrams/tocgi.cc)
add_executable(escape_xml expat/escape_xml.cc expat/escape_xml_main.cc)


if (HAVE_OPENMP)
  set(OPENMP_TARGET_LINK PRIVATE OpenMP::OpenMP_CXX)
endif()  


add_executable(update_database ${update_database_SOURCES})
target_link_libraries(update_database PUBLIC data_lib osm_updater_lib dispatcher_lib expatwrapper_lib output_lib settings_lib ${LIBS} ${OPENMP_TARGET_LINK})

add_executable(update_from_dir ${update_from_dir_SOURCES})
target_link_libraries(update_from_dir PUBLIC data_lib osm_updater_lib dispatcher_lib expatwrapper_lib output_lib settings_lib ${LIBS} ${OPENMP_TARGET_LINK})


add_executable(osm3s_query ${osm3s_query_SOURCES} ${statements_cc_SOURCES} ${output_formats_SOURCES})
target_link_libraries(osm3s_query core_lib data_lib ${LIBS})

add_executable(interpreter ${interpreter_SOURCES} ${statements_cc_SOURCES} ${output_formats_SOURCES})
target_link_libraries(interpreter core_lib data_lib ${LIBS})

add_executable(dispatcher ${dispatcher_SOURCES})
target_link_libraries(dispatcher dispatcher_lib frontend_lib settings_lib ${LIBRT})

add_executable(tagged_nodes ${tagged_nodes_SOURCES})
target_link_libraries(tagged_nodes core_lib data_lib ${LIBS})



