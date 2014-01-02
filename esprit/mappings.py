EXACT = {
    "dynamic_templates" : [
        {
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
    ]
}

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
