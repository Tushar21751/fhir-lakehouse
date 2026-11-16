"""Turn FHIR XML into the same shape as FHIR JSON, so XML loads can reuse the JSON
bronze/silver code.

Only the parts we need are handled properly:
  - <x value="..."/> primitives become plain values (strings - silver casts them)
  - repeating elements become lists. XML can't tell you that an element with a
    single occurrence is a list, so the ones used by Patient/Encounter/Observation/
    Condition/Bundle are listed below (taken from the R4 spec, max cardinality *)
  - narrative div is kept as an xhtml string, extensions keep their url
Primitive extensions (_birthDate etc.) are dropped.
"""
import json
import xml.etree.ElementTree as ET

# datatype elements that are lists wherever they show up
ALWAYS_LIST = {"coding", "given", "prefix", "suffix", "line", "extension", "modifierExtension"}

# lists that hang off any resource
ROOT_LISTS = {"contained", "meta.profile", "meta.security", "meta.tag"}

LIST_PATHS = {
    "Patient": {
        "identifier", "name", "telecom", "address", "photo", "contact", "contact.relationship",
        "contact.telecom", "communication", "generalPractitioner", "link",
    },
    "Encounter": {
        "identifier", "statusHistory", "classHistory", "type", "episodeOfCare", "basedOn",
        "participant", "participant.type", "appointment", "reasonCode", "reasonReference",
        "diagnosis", "account", "hospitalization.dietPreference", "hospitalization.specialCourtesy",
        "hospitalization.specialArrangement", "location",
    },
    "Observation": {
        "identifier", "basedOn", "partOf", "category", "focus", "performer", "interpretation",
        "note", "referenceRange", "referenceRange.appliesTo", "hasMember", "derivedFrom",
        "component", "component.interpretation", "component.referenceRange",
    },
    "Condition": {
        "identifier", "category", "bodySite", "stage", "stage.assessment", "evidence",
        "evidence.code", "evidence.detail", "note",
    },
    "Bundle": {"link", "entry", "entry.link"},
    "OperationOutcome": {"issue", "issue.location", "issue.expression"},
}

# elements whose child is a whole resource
RESOURCE_HOLDERS = {"resource", "contained", "outcome"}


def _tag(el):
    return el.tag.split("}", 1)[1] if el.tag.startswith("{") else el.tag


def _is_list(name, path):
    if name in ALWAYS_LIST:
        return True
    resource_type, _, rest = path.partition(".")
    return rest in ROOT_LISTS or rest in LIST_PATHS.get(resource_type, ())


def _put(obj, name, value, path):
    if _is_list(name, path):
        obj.setdefault(name, []).append(value)
    elif name in obj:
        # repeats but isn't in our list -> still a list
        if not isinstance(obj[name], list):
            obj[name] = [obj[name]]
        obj[name].append(value)
    else:
        obj[name] = value


def _element(el, path):
    obj = {}
    for attr in ("id", "url"):
        if attr in el.attrib:
            obj[attr] = el.attrib[attr]

    for child in el:
        name = _tag(child)
        if name == "div":
            obj["div"] = ET.tostring(child, encoding="unicode")
            continue
        child_path = f"{path}.{name}"
        if name in RESOURCE_HOLDERS and len(child) and "value" not in child.attrib:
            value = resource_to_dict(child[0])
        elif "value" in child.attrib:
            value = child.attrib["value"]
        else:
            value = _element(child, child_path)
        _put(obj, name, value, child_path)
    return obj


def resource_to_dict(el):
    resource_type = _tag(el)
    return {"resourceType": resource_type, **_element(el, resource_type)}


def parse_resource(xml_text):
    if isinstance(xml_text, str):
        xml_text = xml_text.encode("utf-8")
    return resource_to_dict(ET.fromstring(xml_text))


def bundle_entries(xml_text):
    """[(fullUrl, resource_json, (search_mode,)), ...] - same fields bronze reads from json bundles."""
    bundle = parse_resource(xml_text)
    rows = []
    for entry in bundle.get("entry", []):
        resource = entry.get("resource")
        mode = (entry.get("search") or {}).get("mode")
        rows.append((entry.get("fullUrl"), json.dumps(resource) if resource is not None else None, (mode,)))
    return rows


def bundle_entries_or_none(xml_text):
    # used as a spark udf: a broken file gives null (counted in bronze) instead of killing the job
    try:
        return bundle_entries(xml_text)
    except ET.ParseError:
        return None
