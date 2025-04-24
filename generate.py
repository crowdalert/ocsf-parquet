from argparse import ArgumentParser
from pathlib import Path
from typing import Dict, List, Any, Set

from ocsf.schema import to_dict
from ocsf.repository import read_repo
from ocsf.compile.compiler import Compilation

def get_basic_type(type_name: str, types: Dict[str, Any]) -> str:
    """Convert basic OCSF types to Parquet type definitions."""
    if not type_name:
        return "BYTE_ARRAY {} (STRING)"
    
    basic_types = {
        "boolean_t": "BOOLEAN {}",
        "long_t": "INT64 {} (INTEGER(64, true))",
        "integer_t": "INT32 {} (INTEGER(32, true))",
        "float_t": "FLOAT {}",
        "json_t": "BYTE_ARRAY {} (JSON)",
        "timestamp_t": "INT64 {} (TIMESTAMP(MILLIS, false))",
    }
    
    if type_name in basic_types:
        return basic_types[type_name]
    
    # Get the type definition from schema
    type_info = types.get(type_name, {})
    if type_info.get("type"):
        return get_basic_type(type_info["type"], types)
    
    return "BYTE_ARRAY {} (STRING)"

def process_attributes(
    attributes: Dict[str, Any],
    schema: Dict[str, Any],
    processed_objects: Set[str],
    indent: int = 2,
    prefix: str = "",
    processed_attrs: Set[str] = None,
) -> List[str]:
    """Process attributes into Parquet field definitions with recursive object resolution."""
    if not attributes:
        return []
        
    lines = []
    indent_str = " " * indent

    # Initialize processed_attrs set if not provided
    if processed_attrs is None:
        processed_attrs = set()

    for name, attr in attributes.items():
        processed_name = f"{prefix}:{name}"

        if processed_name in processed_attrs:
            continue

        processed_attrs.add(processed_name)

        if not attr:
            continue

        type_name = attr.get("type_name")
        if not type_name:
            type_name = attr.get("type")

        is_array = attr.get("is_array", False)
        
        # Get the complete object definition if this is a complex type
        obj_def = None
        if type_name and type_name in schema.get("objects", {}):
            if type_name in processed_objects:
                continue
            processed_objects.add(type_name)
            obj_def = schema["objects"][type_name]
            
        if obj_def:
            if is_array:
                lines.append(f"{indent_str}optional group {name} (LIST) {{")
                lines.append(f"{indent_str}  repeated group list {{")
                # Process all attributes of the referenced object
                if "attributes" in obj_def:
                    lines.extend(
                        process_attributes(
                            obj_def["attributes"],
                            schema,
                            processed_objects,
                            indent + 4,
                            prefix=processed_name,
                            processed_attrs=processed_attrs,
                        )
                    )

                # If object extends another, process those attributes too
                if "extends" in obj_def and obj_def["extends"] in schema["objects"]:
                    base_obj = schema["objects"][obj_def["extends"]]
                    if "attributes" in base_obj:
                        lines.extend(
                            process_attributes(
                                base_obj["attributes"],
                                schema,
                                processed_objects,
                                indent + 4,
                                prefix=processed_name,
                                processed_attrs=processed_attrs,
                            )
                        )

                lines.append(f"{indent_str}  }}")
                lines.append(f"{indent_str}}}")
            else:
                lines.append(f"{indent_str}optional group {name} {{")
                if "attributes" in obj_def:
                    lines.extend(
                        process_attributes(
                            obj_def["attributes"],
                            schema,
                            processed_objects,
                            indent + 2,
                            prefix=processed_name,
                            processed_attrs=processed_attrs,
                        )
                    )
                if "extends" in obj_def and obj_def["extends"] in schema["objects"]:
                    base_obj = schema["objects"][obj_def["extends"]]
                    if "attributes" in base_obj:
                        lines.extend(
                            process_attributes(
                                base_obj["attributes"],
                                schema,
                                processed_objects,
                                indent + 2,
                                prefix=processed_name,
                                processed_attrs=processed_attrs,
                            )
                        )
                lines.append(f"{indent_str}}}")
            processed_objects.remove(type_name)
        else:
            # Handle basic types
            parquet_type = get_basic_type(type_name, schema["types"])
            if is_array:
                lines.append(f"{indent_str}optional group {name} (LIST) {{")
                lines.append(f"{indent_str}  repeated group list {{")
                lines.append(f"{indent_str}    optional {parquet_type.format('element')};")
                lines.append(f"{indent_str}  }}")
                lines.append(f"{indent_str}}}")
            else:
                lines.append(f"{indent_str}optional {parquet_type.format(name)};")

    return lines

def generate_class_schema(
    class_name: str, class_def: Dict[str, Any], schema: Dict[str, Any]
) -> str:
    """Generate Parquet schema definition for a single class."""
    lines = [f"message {class_name} {{"]
    
    if class_def.get("attributes"):
        processed_objects = set()
        lines.extend(
            process_attributes(class_def["attributes"], schema, processed_objects)
        )
    
    lines.append("}")
    return "\n".join(lines)

def generate_schemas(schema: Dict[str, Any]) -> List[str]:
    """Generate all Parquet schema definitions from an OCSF schema dictionary."""
    schemas = []
    
    for class_name, class_def in schema.get("classes", {}).items():
        if class_def.get("@deprecated"):
            continue

        schemas.append(
            (
                class_def.get("category"),
                class_name,
                generate_class_schema(class_name, class_def, schema),
            )
        )
    
    return schemas

def main():
    parser = ArgumentParser(
        description="Generate Parquet schema from OCSF schema."
    )
    parser.add_argument(
        "repo",
        type=str,
        help="Path to the OCSF repository.",
    )
    parser.add_argument(
        "outdir",
        nargs="?",
        default="output",
        type=str,
        help="Path to write parquet schema files.",
    )
    args = parser.parse_args()
    if not args.repo or not args.outdir:
        parser.print_help()
        return
    if not Path(args.repo).exists() or not Path(args.repo).is_dir():
        print(f"Path {args.repo} does not exist.")
        return
    if Path(args.outdir).exists() and not Path(args.outdir).is_dir():
        print(f"Path {args.outdir} is not a directory.")
        return

    schema = to_dict(Compilation(read_repo(args.repo)).build())

    schemas = generate_schemas(schema)
    
    for category, classname, schema in schemas:
        dest = Path(args.outdir, category, classname).parent
        dest.mkdir(parents=True, exist_ok=True)
        destfile = Path(classname).name
        with open(dest.joinpath(f'{destfile}'), "w+") as f:
            f.write(schema)
        print(f"Generated schema for {classname}.")

if __name__ == "__main__":
    main()
