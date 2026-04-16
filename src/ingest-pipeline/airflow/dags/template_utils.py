import json
from suffix_tree import Tree
from suffix_tree.util import UniqueEndChar
from pprint import pprint
from collections import OrderedDict, defaultdict
from string import Template


KNOWN_BOOL_KEYS = ["is_data_product", "is_qa_qc"]

def total_len(some_l):
    tot = 0
    for dct in some_l:
        for k in dct:
            assert isinstance(k, str), f"Key is not a string in {dct}"
            tot += len(k)
            v = dct[k]
            if isinstance(v, str):
                tot += len(v)
    return tot

def replace_substr(some_l, target_str, repl_str):
    out_l = []
    for dct in some_l:
        rpl_d = {}
        for k in dct:
            v = dct[k]
            if isinstance(v, str):
                rpl_d[k.replace(target_str, repl_str)] = v.replace(target_str, repl_str)
            else:
                rpl_d[k.replace(target_str, repl_str)] = v
        out_l.append(rpl_d)
    return out_l

def apply_substitutions(starting_list, subs_dict, verbose=False):
    working_list = starting_list
    if verbose:
        print(f"initial chars: {total_len(working_list)}")
    for key in subs_dict:
        if verbose:
            print(f"applying {key} -> {subs_dict[key]} ({count_occurrences(working_list, key)} occurrences)")
        working_list = replace_substr(working_list, key, subs_dict[key])
        if verbose:
            print(f"    {total_len(working_list)} chars")
    return working_list

def path_to_str(pth):
    return "".join(str(elt) for elt in pth if not isinstance(elt, UniqueEndChar))

def path_to_str_2(pth):
    ch_l = []
    for elt in pth:
        if isinstance(elt, UniqueEndChar):
            break
        else:
            ch_l.append(str(elt))
    return "".join(ch_l)

def count_occurrences(some_list, target):
    tot = 0
    for dct in some_list:
        for key, val in dct.items():
            tot += key.count(target)
            if isinstance(val, str):
                tot += val.count(target)
    return tot

def keygen(base_str, base_ct=0):
    bs = base_str
    count = 0
    while True:
        yield f"${{{bs}{count}}}"
        count += 1

def build_word_map(starting_list, key_gen, selector, typical_tok_len=6):
    stree = Tree()
    starting_list = starting_list.copy() # to minimize side effects
    sel = selector
    sort_me = []
    for idx, dct in enumerate(starting_list):
        for key, val in dct.items():
            if key == sel and isinstance(val, str):
                stree.add(f"{idx}", val.split())
    for k, lk, path in stree.common_substrings():
        if lk > 0:
            #print(f"{k} {lk}: {path}")
            sort_me.append((k * (len(str(path)) - typical_tok_len), str(path)))
    sort_me.sort(reverse=True)
    #pprint(sort_me)
    best_dct = {}
    for wt, path_str in sort_me:
        best_dct[path_str] = wt
    word_subst_dict = OrderedDict()
    next_tok = next(key_gen)
    for path_str, wt in best_dct.items():
        if wt > 0:
            subst_str = path_str
            if len(subst_str) > len(next_tok):
                word_subst_dict[subst_str] = next_tok
                next_tok = next(key_gen)
    return word_subst_dict

def build_char_map(starting_list, key_gen, selector, typical_tok_len=6):
    stree = Tree()
    starting_list = starting_list.copy() # to minimize side effects
    sel = selector
    sort_me = []
    for idx, dct in enumerate(starting_list):
        for key, val in dct.items():
            if key == sel and isinstance(val, str):
                stree.add(f"{idx}", val)
    for k, lk, path in stree.common_substrings():
        # print(f"found common substring: {k} (length {lk}) in path {path_to_str(path)}")
        if lk > 0 and k > 1:
            path_str = path_to_str(path)
            #print(f"{k} {lk}: {path_str}")
            sort_me.append((k * (len(path_str) - typical_tok_len), path_str, k, lk))
    sort_me.sort(reverse=False)
    # print("sort-me follows")
    # pprint(sort_me)
    # print("sort-me above")
    best_dct = {}
    for wt, path_str, k, lk in sort_me:
        best_dct[path_str] = (wt, k, lk)
    char_subst_dict = OrderedDict()
    next_tok = next(key_gen)
    for path_str, (wt, k, lk) in best_dct.items():
        # print(f"loop {wt} {k} {lk} {path_str} next_tok {next_tok} {char_subst_dict}")
        if wt > 0:
            subst_str = path_str
            #for old_key in char_subst_dict:
            #    subst_str = subst_str.replace(old_key, char_subst_dict[old_key])
            if len(subst_str) > len(next_tok):
                char_subst_dict[subst_str] = next_tok
                next_tok = next(key_gen)
    # print("final char subst dict:")
    # pprint(char_subst_dict)
    return char_subst_dict


def build_reverse_char_map(starting_list, key_gen, selector, typical_tok_len=6):
    rev_list = []
    for dct in starting_list:
        new_dct = {}
        for key, val in dct.items():
            if key == selector and isinstance(val, str):
                new_dct[key] = val[::-1]
        rev_list.append(new_dct)
    char_subst_dict = build_char_map(rev_list, key_gen, selector, typical_tok_len=typical_tok_len)
    rev_char_subst_dict = OrderedDict()
    for key, val in char_subst_dict.items():
        rev_char_subst_dict[key[::-1]] = val
    return rev_char_subst_dict


def fully_template(st, dct):
    prev_st = None
    while st != prev_st:
        prev_st = st
        st = Template(st).substitute(dct)
    return st

def fully_template_dict_list(dict_list, template_dict):
    out_list = []
    for dct in dict_list:
        new_dct = {}
        for key, val in dct.items():
            new_key = fully_template(key, template_dict)
            if isinstance(val, str):
                new_val = fully_template(val, template_dict)
            else:
                new_val = val
            new_dct[new_key] = new_val
        for bool_key in KNOWN_BOOL_KEYS:
            new_dct[bool_key] = bool(new_dct.get(bool_key, False))
        out_list.append(new_dct)
    return out_list


def full_map(dict_list, mapper, key_gen, sel, typical_tok_len=6):
    best_total_len = total_len(dict_list)
    itr = 0
    cum_subst_d = OrderedDict()
    while True:
        subst_d = mapper(dict_list, key_gen, sel, typical_tok_len=typical_tok_len)
        for key in subst_d:
            n_occ = count_occurrences(dict_list, key)
            if n_occ > 1:
                cum_subst_d[key] = subst_d[key]
        dict_list = apply_substitutions(dict_list, cum_subst_d)
        new_total_len = total_len(dict_list) + total_len([cum_subst_d])
        # print(f"{itr}: {new_total_len} vs {best_total_len}")
        if ((new_total_len >= best_total_len - typical_tok_len)
            or itr > 100):
            break
        else:
            best_total_len = new_total_len
            itr += 1
    return cum_subst_d

def invert_and_clean_subst_dict(subst_d):
    inv_subst_d = OrderedDict((val[2:-1], key) 
                              for key, val in subst_d.items())
    clean_d = {}
    for key, val in inv_subst_d.items():
        clean_d[key] = fully_template(val, inv_subst_d)
    return clean_d

def generate_files_template(dict_list):
    typical_tok_len = 6
    files_l = dict_list.copy() # to minimize side effects
    substitution_dict = OrderedDict()

    preclean_dict = OrderedDict()
    preclean_dict["$"] = "$$"
    files_l = apply_substitutions(files_l, preclean_dict)
    # do not append this to the final substitution_dict!

    descr_subst_d = full_map(files_l,
                             build_word_map,
                             keygen("w"),
                             "description",
                             typical_tok_len=typical_tok_len)
    files_l = apply_substitutions(files_l, descr_subst_d)
    substitution_dict.update(descr_subst_d)

    term_subst_d = full_map(files_l,
                            build_char_map,
                            keygen("t"),
                            "edam_term",
                            typical_tok_len=typical_tok_len)
    files_l = apply_substitutions(files_l, term_subst_d)
    substitution_dict.update(term_subst_d)

    path_subst_d = full_map(files_l,
                            build_reverse_char_map,
                            keygen("p"),
                            "rel_path",
                            typical_tok_len=typical_tok_len)
    files_l = apply_substitutions(files_l, path_subst_d)
    substitution_dict.update(path_subst_d)

    # Remove False cases for known boolean keys
    for bool_key in KNOWN_BOOL_KEYS:
        for rec in files_l:
            assert bool_key in rec, f"Expected boolean key {bool_key} missing in files record {rec}"
            if not rec[bool_key]:
                del rec[bool_key]

    key_dict = OrderedDict()
    key_dict["description"] = "${k0}"
    key_dict["edam_term"] = "${k1}"
    key_dict["is_data_product"] = "${k2}"
    key_dict["is_qa_qc"] = "${k3}"
    key_dict["rel_path"] = "${k4}"
    files_l = apply_substitutions(files_l, key_dict, verbose=False)
    substitution_dict.update(key_dict)

    clean_d = invert_and_clean_subst_dict(substitution_dict)

    return clean_d, files_l