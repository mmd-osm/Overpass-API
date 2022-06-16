

#ifdef HAVE_CONFIG_H
#include <config.h>
#undef VERSION
#endif

#ifdef HAVE_LIBOSMIUM

#include "../../expat/escape_xml.h"
#include "../core/settings.h"
#include "../frontend/basic_formats.h"
#include "output_osmium.h"

#include <future>
#include <sys/types.h>
#include <sys/stat.h>

#include <osmium/builder/osm_object_builder.hpp>
#include <osmium/io/opl_output.hpp>
#include <osmium/io/pbf_output.hpp>
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


/*
 * Libosmium/FastCGI repeater Libosmium writes directly to stdout fd rather
 * than using streambuf. This doesn't work out of the box with FastCGI.
 * A dedicated thread is being used to redirect libosmium results to cout
 * via a pipe. Additional overhead due to additional copying, etc.
 * has only insignificant implications in real life.
 *
 * Workaround for https://github.com/osmcode/libosmium/issues/174
 *
 */

void Output_Osmium::setup_pipe()
{
  if (!fastcgi_enabled())
    return;

  saved_stdout = dup(1);

  int rc = pipe(fd);
  if (rc != 0)
    throw File_Error(errno, "", "Output_Osmium::setup_pipe::pipe");

  dup2(fd[1], STDOUT_FILENO);   // redirect stdout to the writing end of the pipe
  close(fd[1]);

  repeater = std::async(std::launch::async, [](int readFd)
  {
    ssize_t len = 0;
    char buffer[PIPE_BUF];

    try {

      while(true)
      {
        len = read(readFd, &buffer, sizeof(buffer));
        if (len < 0) {
          throw File_Error(errno, "", "print_target::osmium::read");
        }

        if (len == 0)
          break;

        std::cout.write(&buffer[0], len);
      }
    } catch (...) {
      close(readFd);
      throw;
    }
    close(readFd);

    std::cout << std::flush;

  }, fd[0]);
}

void Output_Osmium::shutdown_pipe()
{
  if (!fastcgi_enabled())
    return;

  dup2(saved_stdout, 1);
  close(saved_stdout);

  if (repeater.valid())
    repeater.get();
}

bool Output_Osmium::write_http_headers()
{
  if (output_format == "pbf")
     std::cout<<"Content-type: application/vnd.openstreetmap.data.pbf\n" << std::flush;
  else if (output_format == "opl")
     std::cout<<"Content-type: application/vnd.openstreetmap.data.opl\n" << std::flush;
  return true;
}

void Output_Osmium::write_payload_header
    (const std::string& db_dir, const std::string& timestamp, const std::string& area_timestamp)
{
  std::cout << std::flush;

  setup_pipe();

  output_file.reset(new osmium::io::File("", output_format + params));
  header.reset(new osmium::io::Header());

  std::string generator = "Overpass API " + basic_settings().version + " " + basic_settings().source_hash.substr(0, 8);

  header->set("generator", generator);
  header->set("osmosis_replication_timestamp", timestamp);
  writer.reset(new osmium::io::Writer(*output_file, *header, pool, osmium::io::overwrite::allow));
}

void Output_Osmium::write_footer()
{
  try {
    if (writer) {
      writer->operator()(std::move(buffer));
      writer->flush();
      writer->close();
      writer.reset();
    }
  } catch (osmium::io_error&) {
    throw File_Error(1, "", "Output_Osmium::write_footer");
  }

  output_file.reset();
  header.reset();

  shutdown_pipe();
}

void Output_Osmium::display_remark(const std::string& text)
{
  // Intentionally empty
}


void Output_Osmium::display_error(const std::string& text)
{
  // Intentionally empty
}


void Output_Osmium::print_global_bbox(const Bbox_Double& bbox)
{
  // Intentionally empty
}

template <class Builder>
void Output_Osmium::add_tags(Builder & builder, const std::vector< std::pair< std::string, std::string > >* tags)
{
  if (tags == nullptr || tags->empty())
    return;

  osmium::builder::TagListBuilder tag_builder{builder};
  for (auto & [key, value] : *tags)
    tag_builder.add_tag(key, value);
}

template <class Builder, class Id_Type>
void Output_Osmium::add_meta(Builder & builder, const OSM_Element_Metadata_Skeleton< Id_Type >* meta, const user_id_name_t* users)
{
  builder.set_version(meta->version)
         .set_changeset(meta->changeset)
         .set_uid(meta->user_id)
         .set_timestamp(osmium::Timestamp(as_time_t(meta->timestamp)));

  if (meta->user_id > 0 && meta->user_id == prev_user_id) {
    builder.set_user(users->at(prev_user_index).second);
    return;
  }

  auto it = std::lower_bound(users->begin(), users->end(), meta->user_id, User_Comparator_By_Id{});
  if (it != users->end() && meta->user_id == it->first) {
    builder.set_user(it->second);
    prev_user_id = meta->user_id;
    prev_user_index = std::distance(users->begin(), it);
  }
  else {
    builder.set_user("???");
    prev_user_id = 0;
    prev_user_index = 0;
  }
}

void Output_Osmium::add_members(osmium::builder::RelationBuilder & builder,
                                const Relation_Skeleton& skel,
                                const std::map< uint32, std::string >* roles)
{
  osmium::builder::RelationMemberListBuilder rml_builder{buffer, &builder};

  for (uint i = 0; i < skel.members().size(); ++i)
  {
    std::map< uint32, std::string >::const_iterator it = roles->find(skel.members()[i].role);

    // osm3s:  NODE=1, WAY=2, REL=3,
    // osmium: NODE=0, WAY=1, REL=2
    rml_builder.add_member(osmium::nwr_index_to_item_type(skel.members()[i].type - 1),
                          static_cast<osmium::object_id_type>(skel.members()[i].ref.val()),
                          (it != roles->end() ? it->second : "???") );
  }
}

void Output_Osmium::print_item(const Node_Skeleton& skel,
      const Opaque_Geometry& geometry,
      const std::vector< std::pair< std::string, std::string > >* tags,
      const OSM_Element_Metadata_Skeleton< Node::Id_Type >* meta,
      const user_id_name_t* users,
      Output_Mode mode,
      const Feature_Action& action,
      const Node_Skeleton* new_skel,
      const Opaque_Geometry* new_geometry,
      const std::vector< std::pair< std::string, std::string > >* new_tags,
      const OSM_Element_Metadata_Skeleton< Node::Id_Type >* new_meta)
{
  {
    osmium::builder::NodeBuilder builder{buffer};

    osmium::Location loc;

    if (mode & Output_Mode::ID) { }

    if (mode & (Output_Mode::COORDS | Output_Mode::GEOMETRY | Output_Mode::BOUNDS | Output_Mode::CENTER))
    {
      loc.set_lat(geometry.center_lat());
      loc.set_lon(geometry.center_lon());
    }

    builder.set_id(skel.id.val())
          .set_visible(true)
          .set_location(loc);

    if ((mode.mode & (Output_Mode::VERSION | Output_Mode::META)) && meta && users)
      add_meta(builder, meta, users);

    add_tags(builder, tags);
  }
  buffer.commit();

  maybe_flush();
}

void Output_Osmium::print_item(const Way_Skeleton& skel,
      const Opaque_Geometry& geometry,
      const std::vector< std::pair< std::string, std::string > >* tags,
      const OSM_Element_Metadata_Skeleton< Way::Id_Type >* meta,
      const user_id_name_t* users,
      Output_Mode mode,
      const Feature_Action& action,
      const Way_Skeleton* new_skel,
      const Opaque_Geometry* new_geometry,
      const std::vector< std::pair< std::string, std::string > >* new_tags,
      const OSM_Element_Metadata_Skeleton< Way::Id_Type >* new_meta)
{
  {
    osmium::builder::WayBuilder builder{buffer};

    builder.set_id(skel.id.val())
           .set_visible(true);

    if ((mode.mode & (Output_Mode::VERSION | Output_Mode::META)) && meta && users)
      add_meta(builder, meta, users);

    if (((tags == nullptr) || (tags->empty())) &&
        ((mode & (Output_Mode::NDS | Output_Mode::GEOMETRY | Output_Mode::BOUNDS | Output_Mode::CENTER)) == 0))
      ;
    else if (mode & Output_Mode::NDS)
    {
      osmium::builder::WayNodeListBuilder wnl_builder{buffer, &builder};

      for (uint i = 0; i < skel.nds().size(); ++i)
      {
        osmium::Location location;
        osmium::object_id_type ref = skel.nds()[i].val();

        if (geometry.has_faithful_way_geometry() && geometry.way_pos_is_valid(i))
        {
          location.set_lat(geometry.way_pos_lat(i));
          location.set_lon(geometry.way_pos_lon(i));
        }

        wnl_builder.add_node_ref(osmium::NodeRef (ref, location));
      }
    }
    add_tags(builder, tags);
  }
  buffer.commit();

  maybe_flush();
}

void Output_Osmium::print_item(const Relation_Skeleton& skel,
      const Opaque_Geometry& geometry,
      const std::vector< std::pair< std::string, std::string > >* tags,
      const OSM_Element_Metadata_Skeleton< Relation::Id_Type >* meta,
      const std::map< uint32, std::string >* roles,
      const user_id_name_t* users,
      Output_Mode mode,
      const Feature_Action& action,
      const Relation_Skeleton* new_skel,
      const Opaque_Geometry* new_geometry,
      const std::vector< std::pair< std::string, std::string > >* new_tags,
      const OSM_Element_Metadata_Skeleton< Relation::Id_Type >* new_meta)
{
  {
    osmium::builder::RelationBuilder builder{buffer};

    builder.set_id(skel.id.val())
           .set_visible(true);

    if ((mode.mode & (Output_Mode::VERSION | Output_Mode::META)) && meta && users)
      add_meta(builder, meta, users);

    if (((tags == nullptr) || (tags->empty())) &&
        ((mode & (Output_Mode::NDS | Output_Mode::GEOMETRY | Output_Mode::BOUNDS | Output_Mode::CENTER)) == 0))
      ;
    else if (mode & Output_Mode::MEMBERS)
    {
      add_members(builder, skel, roles);
    }
    add_tags(builder, tags);
  }
  buffer.commit();

  maybe_flush();
}

void Output_Osmium::print_item(const Derived_Skeleton& skel,
      const Opaque_Geometry& geometry,
      const std::vector< std::pair< std::string, std::string > >* tags,
      Output_Mode mode,
      const Feature_Action& action)
{
  // Intentionally empty
}

void Output_Osmium::maybe_flush()
{
  try {
    if (buffer.committed() > 800*1024) {
      osmium::memory::Buffer _buffer{1024*1024};
      using std::swap;
      swap(_buffer, buffer);
      writer->operator()(std::move(_buffer));
    }
  } catch (osmium::io_error& e) {
    throw File_Error(1, "", "Output_Osmium::maybe_flush");
  }
}


#endif
