EXACT = {
    "default" : {
        "match" : "*",
        "match_mapping_type": "string",
        "mapping" : {
            "type" : "multi_field",
            "fields" : {
                "{name}" : {"type" : "{dynamic_type}", "index" : "analyzed", "store" : "no"},
                "exact" : {"type" : "{dynamic_type}", "index" : "not_analyzed", "store" : "yes"}
            }
        }
    }
}

{
        	"geo" : {
            	"match" : "canonical_location",
                "mapping" : {
                	"type" : "geo_point"
                }
            }
        }

def make_mapping(type):
    # FIXME: obviously this is not all there is to it
    return {"type" : type}

def dynamic_type_template(name, match, mapping):
    return {
        name : {
            "match" : match,
            "mapping" : mapping
        }
    }

def dynamic_templates(templates):
    return {"dynamic_templates" : templates}

def for_type(typename, mapping):
    return { typename : mapping }

def parent(childtype, parenttype):
    return {
        childtype : {
            "_parent" : {
                "type" : parenttype
            }
        }
    }
