/** Copyright 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016 Roland Olbricht et al.
 *
 * This file is part of Overpass_API.
 *
 * Overpass_API is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * Overpass_API is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Overpass_API.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "node_updater.h"
#include "osmium_updater.h"
#include "relation_updater.h"
#include "tags_updater.h"
#include "way_updater.h"

#include "../../template_db/dispatcher_client.h"
#include "../../template_db/random_file.h"
#include "../../template_db/transaction.h"
#include "../core/settings.h"
#include "../data/abstract_processing.h"
#include "../data/collect_members.h"
#include "../dispatch/resource_manager.h"
#include "../frontend/output.h"

#include <dirent.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <list>
#include <sstream>

#include <osmium/io/any_input.hpp>
#include <osmium/io/detail/output_format.hpp>
#include <osmium/io/file_format.hpp>
#include <osmium/memory/buffer.hpp>
#include <osmium/memory/collection.hpp>
#include <osmium/osm/box.hpp>
#include <osmium/osm/changeset.hpp>
#include <osmium/osm/item_type.hpp>
#include <osmium/osm/location.hpp>
#include <osmium/osm/node.hpp>
#include <osmium/osm/object.hpp>
#include <osmium/osm/relation.hpp>
#include <osmium/osm/tag.hpp>
#include <osmium/osm/timestamp.hpp>
#include <osmium/osm/way.hpp>
#include <osmium/thread/pool.hpp>
#include <osmium/visitor.hpp>

struct Osmium_Updater_Handler: public osmium::handler::Handler {

  uint32 osm_element_count;
  uint flush_limit;
  int state;

  Node_Updater* node_updater;
  Way_Updater* way_updater;
  Relation_Updater* relation_updater;
  Osm_Backend_Callback* callback;

  const int IN_NODES = 1;
  const int IN_WAYS = 2;
  const int IN_RELATIONS = 3;

  Osmium_Updater_Handler(Node_Updater* node_upd_, Way_Updater* way_upd_,
      Relation_Updater* rel_upd_, Osm_Backend_Callback* cb_, uint flush_limit_) :
      osm_element_count(0), flush_limit(flush_limit_), state(0),
      node_updater(node_upd_), way_updater(way_upd_),
      relation_updater(rel_upd_), callback(cb_) {};

  void node(const osmium::Node& n) {

    if (state == 0)
      state = IN_NODES;

    ++osm_element_count;

    OSM_Element_Metadata meta;
    get_meta(n, meta);

    Node node(n.id(), n.location() ? n.location().lat() : 100.0,
                      n.location() ? n.location().lon() : 200.0);

    for (const auto & tag : n.tags())
      node.tags.push_back(make_pair(tag.key(), tag.value()));

    if (n.deleted())
      node_updater->set_id_deleted(n.id(), &meta);
    else
      node_updater->set_node(node, &meta);

    if (osm_element_count >= flush_limit)
    {
      callback->node_elapsed(n.id());
      node_updater->update(callback, true);
      callback->parser_started();
      osm_element_count = 0;
    }
  }

  void way(const osmium::Way& w) {

    move_to_state_in_ways();
    ++osm_element_count;

    Way way(w.id());

    for (const auto & tag : w.tags())
      way.tags.push_back(make_pair(tag.key(), tag.value()));

    for (const auto & nd : w.nodes())
      way.nds.push_back(nd.ref());

    OSM_Element_Metadata meta;
    get_meta(w, meta);

    if (w.deleted())
      way_updater->set_id_deleted(w.id(), &meta);
    else
      way_updater->set_way(way, &meta);

    if (osm_element_count >= flush_limit)
    {
      callback->way_elapsed(w.id());
      way_updater->update(callback, true, node_updater->get_new_skeletons(),
          node_updater->get_attic_skeletons(),
          node_updater->get_new_attic_skeletons());
      callback->parser_started();
      osm_element_count = 0;
    }
  }

  void relation(const osmium::Relation& r) {

    move_to_state_in_relations();
    ++osm_element_count;

    Relation relation(r.id());

    for (const auto & tag : r.tags())
      relation.tags.push_back(make_pair(tag.key(), tag.value()));

    for (const auto & member : r.members())
    {
      Relation_Entry entry;
      entry.ref = member.ref();
      if (member.type() == osmium::item_type::node)
        entry.type = Relation_Entry::NODE;
      else if (member.type() == osmium::item_type::way)
        entry.type = Relation_Entry::WAY;
      else if (member.type() == osmium::item_type::relation)
        entry.type = Relation_Entry::RELATION;
      entry.role = relation_updater->get_role_id(member.role());

      relation.members.push_back(entry);
    }

    OSM_Element_Metadata meta;
    get_meta(r, meta);

    if (r.deleted())
      relation_updater->set_id_deleted(r.id(), &meta);
    else
      relation_updater->set_relation(relation, &meta);

    if (osm_element_count >= flush_limit)
    {
      callback->relation_elapsed(r.id());
      relation_updater->update(callback, node_updater->get_new_skeletons(),
          node_updater->get_attic_skeletons(),
          node_updater->get_new_attic_skeletons(),
          way_updater->get_new_skeletons(), way_updater->get_attic_skeletons(),
          way_updater->get_new_attic_skeletons());
      callback->parser_started();
      osm_element_count = 0;
    }
  }

  void get_meta(const osmium::OSMObject& object, OSM_Element_Metadata& meta) {
    std::string ts_string = object.timestamp().to_iso();
    const char* ts = ts_string.c_str();

    uint64 timestamp = Timestamp(atol(ts),      //year
    atoi(ts + 5),    //month
    atoi(ts + 8),    //day
    atoi(ts + 11),   //hour
    atoi(ts + 14),   //minute
    atoi(ts + 17)    //second
        ).timestamp;

    meta.changeset = object.changeset();
    meta.timestamp = timestamp;
    meta.user_id = object.uid();
    meta.user_name = std::string(object.user());
    meta.version = object.version();
  }

  void finish_updater() {
    if (state == IN_NODES)
      callback->nodes_finished();
    else if (state == IN_WAYS)
      callback->ways_finished();
    else if (state == IN_RELATIONS)
      callback->relations_finished();

    if (state == IN_NODES)
    {
      node_updater->update(callback, false);
      state = IN_WAYS;
    }
    if (state == IN_WAYS)
    {
      way_updater->update(callback, false, node_updater->get_new_skeletons(),
          node_updater->get_attic_skeletons(),
          node_updater->get_new_attic_skeletons());
      state = IN_RELATIONS;
    }
    if (state == IN_RELATIONS)
      relation_updater->update(callback, node_updater->get_new_skeletons(),
          node_updater->get_attic_skeletons(),
          node_updater->get_new_attic_skeletons(),
          way_updater->get_new_skeletons(), way_updater->get_attic_skeletons(),
          way_updater->get_new_attic_skeletons());

    flush();
    callback->parser_succeeded();
  }

  void move_to_state_in_ways() {

    if (state == IN_NODES)
    {
      callback->nodes_finished();
      node_updater->update(callback, false);
      callback->parser_started();
      osm_element_count = 0;
      state = IN_WAYS;
    }
    else if (state == 0)
      state = IN_WAYS;
  }

  void move_to_state_in_relations() {

    if (state == IN_NODES)
    {
      callback->nodes_finished();
      node_updater->update(callback, false);
      callback->parser_started();
      osm_element_count = 0;
      state = IN_RELATIONS;
    }
    else if (state == IN_WAYS)
    {
      callback->ways_finished();
      way_updater->update(callback, false, node_updater->get_new_skeletons(),
          node_updater->get_attic_skeletons(),
          node_updater->get_new_attic_skeletons());
      callback->parser_started();
      osm_element_count = 0;
      state = IN_RELATIONS;
    }
    else if (state == 0)
      state = IN_RELATIONS;
  }
};

void Osmium_Updater::parse_file_completely(FILE* in) {

  this->callback_->parser_started();

  osmium::io::File infile("-", "osm.pbf");

  int exit_code = 0;

  try
  {
    osmium::io::Reader reader(infile);

    Osmium_Updater_Handler osm_updater(node_updater_, way_updater_,
        relation_updater_, callback_, flush_limit);

    while (osmium::memory::Buffer buffer = reader.read())
    {
      osmium::apply(buffer, osm_updater);
    }
    osm_updater.finish_updater();
    reader.close();

  } catch (std::exception& e)
  {
    std::cerr << e.what() << "\n";
    exit_code = 1;
  }
}

Osmium_Updater::Osmium_Updater(Osm_Backend_Callback* callback_,
    const string& data_version_, meta_modes meta_, unsigned int flush_limit_) :
    dispatcher_client(0), meta(meta_) {
  dispatcher_client = new Dispatcher_Client(osm_base_settings().shared_name);
  Logger logger(dispatcher_client->get_db_dir());
  logger.annotated_log("write_start() start version='" + data_version_ + '\'');
  dispatcher_client->write_start();
  logger.annotated_log("write_start() end");
  transaction = new Nonsynced_Transaction(true, true,
      dispatcher_client->get_db_dir(), "");
  {
    ofstream version(
        (dispatcher_client->get_db_dir() + "osm_base_version.shadow").c_str());
    version << data_version_ << '\n';
  }

  this->node_updater_ = new Node_Updater(*transaction, meta);
  this->way_updater_ = new Way_Updater(*transaction, meta);
  this->relation_updater_ = new Relation_Updater(*transaction, meta);
  this->callback_ = callback_;
  this->flush_limit = flush_limit_;
}

Osmium_Updater::Osmium_Updater(Osm_Backend_Callback* callback_, string db_dir,
    const string& data_version_, meta_modes meta_, unsigned int flush_limit_) :
    transaction(0), dispatcher_client(0), db_dir_(db_dir), meta(meta_) {
  {
    ofstream version((db_dir + "osm_base_version").c_str());
    version << data_version_ << '\n';
  }

  this->node_updater_ = new Node_Updater(db_dir, meta);
  this->way_updater_ = new Way_Updater(db_dir, meta);
  this->relation_updater_ = new Relation_Updater(db_dir, meta);
  this->flush_limit = flush_limit_;
  this->callback_ = callback_;
}

void Osmium_Updater::flush() {
  delete node_updater_;
  node_updater_ = new Node_Updater(db_dir_, meta ? keep_meta : only_data);
  delete way_updater_;
  way_updater_ = new Way_Updater(db_dir_, meta);
  delete relation_updater_;
  relation_updater_ = new Relation_Updater(db_dir_, meta);

  if (dispatcher_client)
  {
    delete transaction;
    transaction = 0;
    Logger logger(dispatcher_client->get_db_dir());
    logger.annotated_log("write_commit() start");
    dispatcher_client->write_commit();
    rename(
        (dispatcher_client->get_db_dir() + "osm_base_version.shadow").c_str(),
        (dispatcher_client->get_db_dir() + "osm_base_version").c_str());
    logger.annotated_log("write_commit() end");
    delete dispatcher_client;
    dispatcher_client = 0;
  }
}

Osmium_Updater::~Osmium_Updater() {
  delete node_updater_;
  delete way_updater_;
  delete relation_updater_;

  if (dispatcher_client)
  {
    if (transaction)
      delete transaction;
    Logger logger(dispatcher_client->get_db_dir());
    logger.annotated_log("write_rollback() start");
    dispatcher_client->write_rollback();
    logger.annotated_log("write_rollback() end");
    delete dispatcher_client;
  }
}

