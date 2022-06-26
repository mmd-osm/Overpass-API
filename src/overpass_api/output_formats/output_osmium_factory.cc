
#include "../frontend/output_handler_parser.h"

#ifdef HAVE_CONFIG_H
#include <config.h>
#undef VERSION
#endif

#ifdef HAVE_LIBOSMIUM


#include "output_osmium.h"
#include "../frontend/tokenizer_utils.h"


inline std::string osmium_arguments(Tokenizer_Wrapper* token, Error_Output* error_output, const std::string& output_format) {

  std::string result;

  if (!token)
    return result;

  if (**token == "(") {

    do
    {
      ++(*token);

      std::string arg = get_text_token(*token, error_output, "Optional values include 'geom'");
      if (arg == "geom") {
        result += ",locations_on_ways=yes";
      }

      else if (arg == "lz4" && output_format == "pbf")
      {
#ifdef OSMIUM_WITH_LZ4
        result += ",pbf_compression=lz4";
#else
        error_output->add_static_error("PBF with LZ4 compression is not available on this instance", token->line_col().first);
#endif
      }

      clear_until_after(*token, error_output, ",", ")", false);
    } while (token->good() && **token == ",");

    clear_until_after(*token, error_output, ")");
  }

  return result;
}

// ------------------------------------------------------------------------

class Output_Osmium_PBF_Generator : public Output_Handler_Parser
{
public:
  Output_Osmium_PBF_Generator() : Output_Handler_Parser("pbf") {}

  Output_Handler* new_output_handler(const std::map< std::string, std::string >& input_params,
      Tokenizer_Wrapper* token, Error_Output* error_output) override;

  static Output_Osmium_PBF_Generator singleton;
};


Output_Osmium_PBF_Generator Output_Osmium_PBF_Generator::singleton;


Output_Handler* Output_Osmium_PBF_Generator::new_output_handler(const std::map< std::string, std::string >& input_params,
                                                         Tokenizer_Wrapper* token, Error_Output* error_output)
{
  auto params = osmium_arguments(token, error_output, "pbf");
  return new Output_Osmium("pbf", params);
}

// ------------------------------------------------------------------------

class Output_Osmium_OPL_Generator : public Output_Handler_Parser
{
public:
  Output_Osmium_OPL_Generator() : Output_Handler_Parser("opl") {}

  Output_Handler* new_output_handler(const std::map< std::string, std::string >& input_params,
      Tokenizer_Wrapper* token, Error_Output* error_output) override;

  static Output_Osmium_OPL_Generator singleton;
};


Output_Osmium_OPL_Generator Output_Osmium_OPL_Generator::singleton;


Output_Handler* Output_Osmium_OPL_Generator::new_output_handler(const std::map< std::string, std::string >& input_params,
                                                         Tokenizer_Wrapper* token, Error_Output* error_output)
{
  auto params = osmium_arguments(token, error_output, "opl");
  return new Output_Osmium("opl", params);
}

// ------------------------------------------------------------------------

class Output_Osmium_XML_Generator : public Output_Handler_Parser
{
public:
  Output_Osmium_XML_Generator() : Output_Handler_Parser("osmxml") {}

  Output_Handler* new_output_handler(const std::map< std::string, std::string >& input_params,
      Tokenizer_Wrapper* token, Error_Output* error_output) override;

  static Output_Osmium_XML_Generator singleton;
};


Output_Osmium_XML_Generator Output_Osmium_XML_Generator::singleton;


Output_Handler* Output_Osmium_XML_Generator::new_output_handler(const std::map< std::string, std::string >& input_params,
                                                         Tokenizer_Wrapper* token, Error_Output* error_output)
{
  auto params = osmium_arguments(token, error_output, "xml");
  return new Output_Osmium("xml", params);
}

#endif
