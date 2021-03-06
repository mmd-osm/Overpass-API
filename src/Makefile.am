AM_CPPFLAGS = -I$(top_srcdir)/third_party/libosmium/include -I$(top_srcdir)/third_party/protozero/include
AM_CXXFLAGS = -DGIT_VERSION="\"$(shell git log -1 --pretty='%h')\""

bin_mandatory = bin/osm3s_query bin/dispatcher bin/update_database bin/update_from_dir bin/create_tagged_nodes bin/map_demo bin/dump_file bin/export_tables bin/import_tables
bin_script_mandatory = \
  bin/apply_osc_to_db.sh\
  bin/download_clone.sh\
  bin/fetch_osc.sh\
  bin/fix_system_settings.sh\
  bin/reboot.sh\
  bin/rules_delta_loop.sh\
  bin/rules_loop.sh
cgi_bin_mandatory = cgi-bin/interpreter cgi-bin/timestamp
cgi_bin_script_mandatory = cgi-bin/kill_my_queries cgi-bin/map cgi-bin/status

bin_customoutput = bin/uncgi bin/escape_xml
bin_script_customoutput =
cgi_bin_customoutput =
cgi_bin_script_customoutput = cgi-bin/template

bin_clone =
bin_script_clone = bin/compress_clone.sh bin/clone.sh
cgi_bin_clone =
cgi_bin_script_clone = cgi-bin/trigger_clone

cgi_bin_augmented_diffs =
cgi_bin_script_augmented_diffs = cgi-bin/augmented_diff cgi-bin/augmented_diff_status cgi-bin/augmented_state_by_date

bin_xapi_layer = bin/translate_xapi

cgi_bin_xapi_layer = 
cgi_bin_script_xapi_layer = cgi-bin/convert_xapi cgi-bin/xapi cgi-bin/xapi_meta

bin_openlayers = bin/tocgi bin/uncgi bin/escape_xml
bin_script_openlayers =
cgi_bin_openlayers =
cgi_bin_script_openlayers = cgi-bin/convert

bin_public_transport = bin/uncgi bin/escape_xml


bin_convenience =
bin_script_convenience = bin/fetch_osc_and_apply.sh bin/init_osm3s.sh bin/run_osm3s_minutely.sh
cgi_bin_convenience =
cgi_bin_script_convenience = cgi-bin/ping cgi-bin/sitemap

bindir = ${prefix}/bin
bin_PROGRAMS = \
  ${bin_mandatory} \
  ${bin_customoutput} \
  ${bin_clone} \
  ${bin_augmented_diffs} \
  ${bin_xapi_layer} \
  ${bin_openlayers} \
  ${bin_public_transport} \
  ${bin_convenience}
  
dist_bin_SCRIPTS = \
  ${bin_script_mandatory} \
  ${bin_script_customoutput} \
  ${bin_script_clone} \
  ${bin_script_augmented_diffs} \
  ${bin_script_xapi_layer} \
  ${bin_script_openlayers} \
  ${bin_script_convenience}
  

cgibindir = ${prefix}/cgi-bin
cgibin_PROGRAMS = \
  ${cgi_bin_mandatory} \
  ${cgi_bin_customoutput} \
  ${cgi_bin_clone} \
  ${cgi_bin_augmented_diffs} \
  ${cgi_bin_xapi_layer} \
  ${cgi_bin_openlayers} \
  ${cgi_bin_convenience}

dist_cgibin_SCRIPTS = \
  ${cgi_bin_script_mandatory} \
  ${cgi_bin_script_customoutput} \
  ${cgi_bin_script_clone} \
  ${cgi_bin_script_augmented_diffs} \
  ${cgi_bin_script_xapi_layer} \
  ${cgi_bin_script_openlayers} \
  ${cgi_bin_script_convenience}

templatesdir = ${prefix}/templates
dist_templates_DATA = templates/default.wiki templates/base.wiki

noinst_LTLIBRARIES = libcore.la libdata.la libdispatcher.la libdispatcherclient.la libexpatwrapper.la libfrontend.la liboutput.la libsettings.la

statements_cc = \
  overpass_api/data/bbox_filter.cc \
  overpass_api/statements/aggregators.cc \
  overpass_api/statements/area_query.cc \
  overpass_api/statements/around.cc \
  overpass_api/statements/bbox_query.cc \
  overpass_api/statements/binary_operators.cc \
  overpass_api/statements/changed.cc \
  overpass_api/statements/compare.cc \
  overpass_api/statements/complete.cc \
  overpass_api/statements/convert.cc \
  overpass_api/statements/coord_query.cc \
  overpass_api/statements/difference.cc \
  overpass_api/statements/evaluator.cc \
  overpass_api/statements/explicit_geometry.cc \
  overpass_api/statements/filter.cc \
  overpass_api/statements/for.cc \
  overpass_api/statements/foreach.cc \
  overpass_api/statements/geometry_endomorphisms.cc \
  overpass_api/statements/id_query.cc \
  overpass_api/statements/if.cc \
  overpass_api/statements/item.cc \
  overpass_api/statements/item_geometry.cc \
  overpass_api/statements/localize.cc \
  overpass_api/statements/make_area.cc \
  overpass_api/statements/make.cc \
  overpass_api/statements/map_to_area.cc \
  overpass_api/statements/newer.cc \
  overpass_api/statements/osm_script.cc \
  overpass_api/statements/per_member.cc \
  overpass_api/statements/pivot.cc \
  overpass_api/statements/polygon_query.cc \
  overpass_api/statements/print.cc \
  overpass_api/statements/query.cc \
  overpass_api/statements/recurse.cc \
  overpass_api/statements/retro.cc \
  overpass_api/statements/runtime_value.cc \
  overpass_api/statements/set_list_operators.cc \
  overpass_api/statements/set_prop.cc \
  overpass_api/statements/statement.cc \
  overpass_api/statements/string_endomorphisms.cc \
  overpass_api/statements/tag_value.cc \
  overpass_api/statements/ternary_operator.cc \
  overpass_api/statements/timeline.cc \
  overpass_api/statements/unary_functions.cc \
  overpass_api/statements/unary_operators.cc \
  overpass_api/statements/union.cc \
  overpass_api/statements/user.cc \
  expat/escape_json.cc \
  expat/escape_xml.cc

output_formats_cc = \
  overpass_api/output_formats/output_csv.cc \
  overpass_api/output_formats/output_csv_factory.cc \
  overpass_api/output_formats/output_custom.cc \
  overpass_api/output_formats/output_custom_factory.cc \
  overpass_api/output_formats/output_json.cc \
  overpass_api/output_formats/output_json_factory.cc \
  overpass_api/output_formats/output_osmium.cc \
  overpass_api/output_formats/output_osmium_factory.cc \
  overpass_api/output_formats/output_xml.cc \
  overpass_api/output_formats/output_xml_factory.cc \
  overpass_api/output_formats/output_popup.cc \
  overpass_api/output_formats/output_popup_factory.cc

libcore_la_SOURCES = overpass_api/frontend/output_handler_parser.cc overpass_api/statements/statement_dump.cc expat/map_ql_input.cc
libcore_la_LIBADD = libdispatcher.la libexpatwrapper.la libsettings.la libfrontend.la
libdata_la_SOURCES = overpass_api/data/collect_members.cc overpass_api/data/diff_set.cc overpass_api/data/geometry_from_quad_coords.cc overpass_api/data/relation_geometry_store.cc overpass_api/data/set_comparison.cc overpass_api/data/way_geometry_store.cc
libdata_la_LIBADD =
libdispatcherclient_la_SOURCES = template_db/dispatcher_client.cc
libdispatcherclient_la_LIBADD =
libdispatcher_la_SOURCES = overpass_api/dispatch/resource_manager.cc overpass_api/osm-backend/area_updater.cc
libdispatcher_la_LIBADD = libdispatcherclient.la
libexpatwrapper_la_SOURCES = expat/expat_justparse_interface.cc
libexpatwrapper_la_LIBADD = -lexpat
libfrontend_la_SOURCES = overpass_api/frontend/cgi-helper.cc overpass_api/frontend/user_interface.cc
libfrontend_la_LIBADD = liboutput.la
liboutput_la_SOURCES = overpass_api/frontend/output.cc
liboutput_la_LIBADD =
libsettings_la_SOURCES = overpass_api/core/settings.cc
libsettings_la_LIBADD =

osm_updater_cc = overpass_api/osm-backend/meta_updater.cc overpass_api/osm-backend/basic_updater.cc overpass_api/osm-backend/node_updater.cc overpass_api/osm-backend/way_updater.cc overpass_api/osm-backend/relation_updater.cc overpass_api/osm-backend/osm_updater.cc overpass_api/osm-backend/osmium_updater.cc overpass_api/core/four_field_index.cc overpass_api/core/geometry.cc expat/escape_xml.cc



bin_update_database_SOURCES = ${osm_updater_cc} overpass_api/osm-backend/update_database.cc template_db/types.cc template_db/zlib_wrapper.cc template_db/lz4_wrapper.cc
bin_update_database_LDADD = libdata.la libdispatcher.la libexpatwrapper.la liboutput.la libsettings.la @COMPRESS_LIBS@
bin_update_database_CXXFLAGS = $(CXXFLAGS) @OPENMP_FLAG@ @BZ2_LIBS@
bin_update_from_dir_SOURCES = ${osm_updater_cc} overpass_api/osm-backend/update_from_dir.cc template_db/types.cc template_db/zlib_wrapper.cc template_db/lz4_wrapper.cc
bin_update_from_dir_LDADD = libdata.la libdispatcher.la libexpatwrapper.la liboutput.la libsettings.la @COMPRESS_LIBS@
bin_update_from_dir_CXXFLAGS = $(CXXFLAGS) @OPENMP_FLAG@ @BZ2_LIBS@
bin_osm3s_query_SOURCES = ${statements_cc} ${output_formats_cc} overpass_api/frontend/basic_formats.cc overpass_api/frontend/output_handler.cc overpass_api/frontend/console_output.cc overpass_api/frontend/web_output.cc overpass_api/dispatch/osm3s_query.cc overpass_api/osm-backend/clone_database.cc overpass_api/core/four_field_index.cc overpass_api/core/geometry.cc overpass_api/dispatch/scripting_core.cc overpass_api/dispatch/dispatcher_stub.cc template_db/types.cc overpass_api/frontend/decode_text.cc overpass_api/frontend/map_ql_parser.cc overpass_api/frontend/tokenizer_utils.cc template_db/zlib_wrapper.cc template_db/lz4_wrapper.cc
bin_osm3s_query_LDADD = libcore.la libdata.la @COMPRESS_LIBS@ @ICU_LIBS@ @PCRE_LIBS@ @PTHREAD_LIBS@
bin_dispatcher_SOURCES = template_db/dispatcher.cc template_db/file_tools.cc template_db/transaction_insulator.cc template_db/types.cc overpass_api/dispatch/dispatcher_server.cc
bin_dispatcher_LDADD = libdispatcher.la libfrontend.la libsettings.la



bin_create_tagged_nodes_SOURCES = overpass_api/osm-backend/create_tagged_nodes.cc template_db/types.cc template_db/zlib_wrapper.cc template_db/lz4_wrapper.cc
bin_create_tagged_nodes_LDADD = libcore.la libdata.la @COMPRESS_LIBS@

bin_map_demo_SOURCES = overpass_api/osm-backend/map_demo.cc ${statements_cc} ${output_formats_cc} overpass_api/frontend/basic_formats.cc overpass_api/frontend/output_handler.cc overpass_api/frontend/console_output.cc overpass_api/frontend/web_output.cc overpass_api/osm-backend/clone_database.cc overpass_api/core/four_field_index.cc overpass_api/core/geometry.cc overpass_api/dispatch/scripting_core.cc overpass_api/dispatch/dispatcher_stub.cc template_db/types.cc overpass_api/frontend/decode_text.cc overpass_api/frontend/map_ql_parser.cc overpass_api/frontend/tokenizer_utils.cc template_db/zlib_wrapper.cc template_db/lz4_wrapper.cc
bin_map_demo_LDADD = libcore.la libdata.la @COMPRESS_LIBS@ @ICU_LIBS@ @PCRE_LIBS@ @PTHREAD_LIBS@

bin_dump_file_SOURCES = overpass_api/osm-backend/dump_file.cc   ${statements_cc} ${output_formats_cc} overpass_api/frontend/basic_formats.cc overpass_api/frontend/output_handler.cc overpass_api/frontend/console_output.cc overpass_api/frontend/web_output.cc overpass_api/osm-backend/clone_database.cc overpass_api/core/four_field_index.cc overpass_api/core/geometry.cc overpass_api/dispatch/scripting_core.cc overpass_api/dispatch/dispatcher_stub.cc template_db/types.cc overpass_api/frontend/decode_text.cc overpass_api/frontend/map_ql_parser.cc overpass_api/frontend/tokenizer_utils.cc template_db/zlib_wrapper.cc template_db/lz4_wrapper.cc
bin_dump_file_LDADD = libcore.la libdata.la @COMPRESS_LIBS@ @ICU_LIBS@ @PCRE_LIBS@ @PTHREAD_LIBS@


bin_export_tables_SOURCES = overpass_api/osm-backend/export_tables.cc   ${statements_cc} ${output_formats_cc} overpass_api/frontend/basic_formats.cc overpass_api/frontend/output_handler.cc overpass_api/frontend/console_output.cc overpass_api/frontend/web_output.cc overpass_api/osm-backend/clone_database.cc overpass_api/core/four_field_index.cc overpass_api/core/geometry.cc overpass_api/dispatch/scripting_core.cc overpass_api/dispatch/dispatcher_stub.cc template_db/types.cc overpass_api/frontend/decode_text.cc overpass_api/frontend/map_ql_parser.cc overpass_api/frontend/tokenizer_utils.cc template_db/zlib_wrapper.cc template_db/lz4_wrapper.cc
bin_export_tables_LDADD = libcore.la libdata.la @COMPRESS_LIBS@ @ICU_LIBS@ @PCRE_LIBS@ @PTHREAD_LIBS@

bin_import_tables_SOURCES = overpass_api/osm-backend/import_tables.cc   ${statements_cc} ${output_formats_cc} overpass_api/frontend/basic_formats.cc overpass_api/frontend/output_handler.cc overpass_api/frontend/console_output.cc overpass_api/frontend/web_output.cc overpass_api/osm-backend/clone_database.cc overpass_api/core/four_field_index.cc overpass_api/core/geometry.cc overpass_api/dispatch/scripting_core.cc overpass_api/dispatch/dispatcher_stub.cc template_db/types.cc overpass_api/frontend/decode_text.cc overpass_api/frontend/map_ql_parser.cc overpass_api/frontend/tokenizer_utils.cc template_db/zlib_wrapper.cc template_db/lz4_wrapper.cc
bin_import_tables_LDADD = libcore.la libdata.la @COMPRESS_LIBS@ @ICU_LIBS@ @PCRE_LIBS@ @PTHREAD_LIBS@

cgi_bin_interpreter_SOURCES = ${statements_cc} ${output_formats_cc} overpass_api/frontend/basic_formats.cc overpass_api/frontend/output_handler.cc overpass_api/dispatch/web_query.cc overpass_api/core/four_field_index.cc overpass_api/core/geometry.cc overpass_api/dispatch/scripting_core.cc overpass_api/dispatch/dispatcher_stub.cc template_db/types.cc overpass_api/frontend/decode_text.cc overpass_api/frontend/map_ql_parser.cc overpass_api/frontend/tokenizer_utils.cc overpass_api/frontend/web_output.cc template_db/zlib_wrapper.cc template_db/lz4_wrapper.cc
cgi_bin_interpreter_LDADD = libcore.la libdata.la @COMPRESS_LIBS@ @FASTCGI_LIBS@ @ICU_LIBS@ @PCRE_LIBS@ @PTHREAD_LIBS@

cgi_bin_timestamp_SOURCES = overpass_api/dispatch/db_timestamp.cc overpass_api/frontend/basic_formats.cc overpass_api/frontend/decode_text.cc overpass_api/frontend/web_output.cc expat/escape_xml.cc template_db/types.cc
cgi_bin_timestamp_LDADD = libdispatcherclient.la libsettings.la
#cgi_bin_timestamp_SOURCES = overpass_api/frontend/basic_formats.cc overpass_api/dispatch/db_timestamp.cc overpass_api/core/four_field_index.cc overpass_api/core/geometry.cc overpass_api/dispatch/dispatcher_stub.cc template_db/types.cc template_db/zlib_wrapper.cc template_db/lz4_wrapper.cc
#cgi_bin_timestamp_LDADD = libdispatcher.la libsettings.la libweboutput.la @COMPRESS_LIBS@


escape_xml_cc = expat/escape_xml.cc

bin_uncgi_SOURCES = pt_diagrams/uncgi.cc
bin_uncgi_LDADD =
bin_tocgi_SOURCES = pt_diagrams/tocgi.cc
bin_tocgi_LDADD =
bin_escape_xml_SOURCES = expat/escape_xml.cc expat/escape_xml_main.cc
bin_escape_xml_LDADD =
bin_translate_xapi_SOURCES = expat/translate_xapi.cc expat/escape_xml.cc
bin_translate_xapi_LDADD = 

distdir = osm-3s_v0.7.59

nobase_dist_HEADERS = \
  expat/escape_json.h\
  expat/escape_xml.h\
  expat/expat_justparse_interface.h\
  expat/map_ql_input.h\
  overpass_api/core/basic_types.h\
  overpass_api/core/datatypes.h\
  overpass_api/core/four_field_index.h\
  overpass_api/core/geometry.h\
  overpass_api/core/index_computations.h\
  overpass_api/core/parsed_query.h\
  overpass_api/core/settings.h\
  overpass_api/core/type_area.h\
  overpass_api/core/type_node.h\
  overpass_api/core/type_relation.h\
  overpass_api/core/type_tags.h\
  overpass_api/core/type_way.h\
  overpass_api/data/abstract_processing.h\
  overpass_api/data/bbox_filter.h\
  overpass_api/data/collect_items.h\
  overpass_api/data/collect_members.h\
  overpass_api/data/diff_set.h\
  overpass_api/data/filenames.h\
  overpass_api/data/filter_by_tags.h\
  overpass_api/data/filter_ids_by_tags.h\
  overpass_api/data/geometry_from_quad_coords.h\
  overpass_api/data/meta_collector.h\
  overpass_api/data/regular_expression.h\
  overpass_api/data/relation_geometry_store.h\
  overpass_api/data/set_comparison.h\
  overpass_api/data/tag_store.h\
  overpass_api/data/user_data_cache.h\
  overpass_api/data/utils.h\
  overpass_api/data/way_geometry_store.h\
  overpass_api/dispatch/dispatcher_stub.h\
  overpass_api/dispatch/resource_manager.h\
  overpass_api/dispatch/scripting_core.h\
  overpass_api/frontend/basic_formats.h\
  overpass_api/frontend/cgi-helper.h\
  overpass_api/frontend/console_output.h\
  overpass_api/frontend/decode_text.h\
  overpass_api/frontend/map_ql_parser.h\
  overpass_api/frontend/output.h\
  overpass_api/frontend/output_handler.h\
  overpass_api/frontend/output_handler_parser.h\
  overpass_api/frontend/tokenizer_utils.h\
  overpass_api/frontend/user_interface.h\
  overpass_api/frontend/web_output.h\
  overpass_api/osm-backend/area_updater.h\
  overpass_api/osm-backend/basic_updater.h\
  overpass_api/osm-backend/clone_database.h\
  overpass_api/osm-backend/meta_updater.h\
  overpass_api/osm-backend/node_updater.h\
  overpass_api/osm-backend/osm_updater.h\
  overpass_api/osm-backend/osmium_updater.h\
  overpass_api/osm-backend/relation_updater.h\
  overpass_api/osm-backend/tags_updater.h\
  overpass_api/osm-backend/way_updater.h\
  overpass_api/output_formats/output_csv.h\
  overpass_api/output_formats/output_custom.h\
  overpass_api/output_formats/output_json.h\
  overpass_api/output_formats/output_popup.h\
  overpass_api/output_formats/output_xml.h\
  overpass_api/statements/aggregators.h\
  overpass_api/statements/area_query.h\
  overpass_api/statements/around.h\
  overpass_api/statements/bbox_query.h\
  overpass_api/statements/binary_operators.h\
  overpass_api/statements/changed.h\
  overpass_api/statements/compare.h\
  overpass_api/statements/complete.h\
  overpass_api/statements/convert.h\
  overpass_api/statements/coord_query.h\
  overpass_api/statements/difference.h\
  overpass_api/statements/evaluator.h\
  overpass_api/statements/explicit_geometry.h\
  overpass_api/statements/filter.h\
  overpass_api/statements/for.h\
  overpass_api/statements/foreach.h\
  overpass_api/statements/geometry_endomorphisms.h\
  overpass_api/statements/id_query.h\
  overpass_api/statements/if.h\
  overpass_api/statements/item.h\
  overpass_api/statements/item_geometry.h\
  overpass_api/statements/localize.h\
  overpass_api/statements/make_area.h\
  overpass_api/statements/make.h\
  overpass_api/statements/map_to_area.h\
  overpass_api/statements/newer.h\
  overpass_api/statements/osm_script.h\
  overpass_api/statements/per_member.h\
  overpass_api/statements/pivot.h\
  overpass_api/statements/polygon_query.h\
  overpass_api/statements/print.h\
  overpass_api/statements/query.h\
  overpass_api/statements/recurse.h\
  overpass_api/statements/retro.h\
  overpass_api/statements/runtime_value.h\
  overpass_api/statements/set_list_operators.h\
  overpass_api/statements/set_prop.h\
  overpass_api/statements/statement_dump.h\
  overpass_api/statements/statement.h\
  overpass_api/statements/string_endomorphisms.h\
  overpass_api/statements/tag_value.h\
  overpass_api/statements/ternary_operator.h\
  overpass_api/statements/timeline.h\
  overpass_api/statements/unary_functions.h\
  overpass_api/statements/unary_operators.h\
  overpass_api/statements/union.h\
  overpass_api/statements/user.h\
  template_db/block_backend.h\
  template_db/dispatcher_client.h\
  template_db/dispatcher.h\
  template_db/file_blocks.h\
  template_db/file_blocks_index.h\
  template_db/file_tools.h\
  template_db/lz4_wrapper.h\
  template_db/random_file.h\
  template_db/random_file_index.h\
  template_db/transaction.h\
  template_db/transaction_insulator.h\
  template_db/types.h\
  template_db/zlib_wrapper.h

EXTRA_DIST = \
  html/command_line.html\
  html/full_installation.html\
  html/index.html\
  html/no_frills.html\
  html/open_layers_bbox.html\
  html/open_layers_fixed.html\
  html/open_layers_mashup.html\
  html/open_layers_multilayer.html\
  html/open_layers_popup.html\
  html/output_formats.html\
  html/overpass.js\
  html/query_form.html\
  html/technical_overview.html\
  rules/areas.osm3s\
  rules/areas_delta.osm3s

if COND_TESTS
       MAYBE_TESTS = test-bin
endif

SUBDIRS = $(MAYBE_TESTS)


