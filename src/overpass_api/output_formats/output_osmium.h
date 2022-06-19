#ifndef DE__OSM3S___OVERPASS_API__OUTPUT_FORMATS__OUTPUT_OSMIUM_H
#define DE__OSM3S___OVERPASS_API__OUTPUT_FORMATS__OUTPUT_OSMIUM_H


#include "../core/datatypes.h"
#include "../core/geometry.h"
#include "../frontend/output_handler.h"
#include "../../template_db/types.h"

#ifdef HAVE_CONFIG_H
#include <config.h>
#undef VERSION
#endif

#ifdef HAVE_LIBOSMIUM

#include <future>

#include <osmium/builder/osm_object_builder.hpp>
#include <osmium/io/opl_output.hpp>
#include <osmium/io/pbf_output.hpp>
#include <osmium/io/xml_output.hpp>
#include <osmium/io/error.hpp>
#include <osmium/io/gzip_compression.hpp>
#include <osmium/io/header.hpp>
#include <osmium/memory/buffer.hpp>
#include <osmium/builder/attr.hpp>
#include <osmium/osm/box.hpp>
#include <osmium/osm/item_type.hpp>
#include <osmium/osm/location.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/relation.hpp>
#include <osmium/osm/timestamp.hpp>
#include <osmium/osm/types.hpp>
#include <osmium/osm/way.hpp>
#include <osmium/osm.hpp>
#include <osmium/osm/types.hpp>


class Output_Osmium : public Output_Handler
{
public:
  Output_Osmium(std::string output_format_, std::string params_) :
        output_format(output_format_), params(params_), writer(nullptr),
        output_file(nullptr), header(nullptr) {}



  bool write_http_headers() override;
  void write_payload_header(const std::string& db_dir,
                            const std::string& timestamp, const std::string& area_timestamp) override;
  void write_footer() override;
  void display_remark(const std::string& text) override;
  void display_error(const std::string& text) override;

  void print_global_bbox(const Bbox_Double& bbox) override;

  void print_item(const Node_Skeleton& skel,
      const Opaque_Geometry& geometry,
      const std::vector< std::pair< std::string, std::string > >* tags,
      const OSM_Element_Metadata_Skeleton< Node::Id_Type >* meta,
      const user_id_name_t* users,
      Output_Mode mode,
      const Feature_Action& action = keep,
      const Node_Skeleton* new_skel = nullptr,
      const Opaque_Geometry* new_geometry = nullptr,
      const std::vector< std::pair< std::string, std::string > >* new_tags = nullptr,
      const OSM_Element_Metadata_Skeleton< Node::Id_Type >* new_meta = nullptr) override;

  void print_item(const Way_Skeleton& skel,
      const Opaque_Geometry& geometry,
      const std::vector< std::pair< std::string, std::string > >* tags,
      const OSM_Element_Metadata_Skeleton< Way::Id_Type >* meta,
      const user_id_name_t* users,
      Output_Mode mode,
      const Feature_Action& action = keep,
      const Way_Skeleton* new_skel = nullptr,
      const Opaque_Geometry* new_geometry = nullptr,
      const std::vector< std::pair< std::string, std::string > >* new_tags = nullptr,
      const OSM_Element_Metadata_Skeleton< Way::Id_Type >* new_meta = nullptr) override;

  void print_item(const Relation_Skeleton& skel,
      const Opaque_Geometry& geometry,
      const std::vector< std::pair< std::string, std::string > >* tags,
      const OSM_Element_Metadata_Skeleton< Relation::Id_Type >* meta,
      const std::map< uint32, std::string >* roles,
      const user_id_name_t* users,
      Output_Mode mode,
      const Feature_Action& action = keep,
      const Relation_Skeleton* new_skel = nullptr,
      const Opaque_Geometry* new_geometry = nullptr,
      const std::vector< std::pair< std::string, std::string > >* new_tags = nullptr,
      const OSM_Element_Metadata_Skeleton< Relation::Id_Type >* new_meta = nullptr) override;

  void print_item(const Derived_Skeleton& skel,
      const Opaque_Geometry& geometry,
      const std::vector< std::pair< std::string, std::string > >* tags,
      Output_Mode mode,
      const Feature_Action& action = keep) override;

private:
  void maybe_flush();
  void setup_pipe();
  void shutdown_pipe();

  template <class Builder>
  void add_tags(Builder & builder, const std::vector< std::pair< std::string, std::string > >* tags);

  template <class Builder, class Id_Type>
  void add_meta(Builder & builder, const OSM_Element_Metadata_Skeleton< Id_Type >& meta, const user_id_name_t& users);

  void add_members(osmium::builder::RelationBuilder & builder, const Relation_Skeleton& skel,
                   const std::map< uint32, std::string >* roles);

  std::string output_format;
  std::string params;
  osmium::memory::Buffer buffer{1024*1024};
  osmium::thread::Pool pool{};
  std::unique_ptr<osmium::io::Writer> writer;
  std::unique_ptr<osmium::io::File> output_file;
  std::unique_ptr<osmium::io::Header> header;
  std::future<void> repeater;
  int saved_stdout;
  int fd[2];
};

#endif

#endif
