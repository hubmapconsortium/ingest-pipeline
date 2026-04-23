from suffix_tree import Tree
from suffix_tree.util import Path as SuffixTreePath, UniqueEndChar
from collections import OrderedDict
from string import Template
from typing import Generator, Callable


DictList = list[dict[str, str]]
MappingFunc = Callable[[DictList, Generator[str, None, None], str, int], OrderedDict[str, str]]


KNOWN_BOOL_KEYS = ["is_data_product", "is_qa_qc"]


def total_len(some_l: DictList) -> int:
    tot = 0
    for dct in some_l:
        for k in dct:
            assert isinstance(k, str), f"Key is not a string in {dct}"
            tot += len(k)
            v = dct[k]
            if isinstance(v, str):
                tot += len(v)
    return tot


def replace_substr(some_l: DictList, target_str: str, repl_str: str) -> DictList:
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


def apply_substitutions(starting_list: DictList, subs_dict: OrderedDict[str, str],
                        verbose=False) -> DictList:
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

def path_to_str(pth: SuffixTreePath) -> str:
    return "".join(str(elt) for elt in pth if not isinstance(elt, UniqueEndChar))


def count_occurrences(some_list: DictList, target: str, stop_after=None) -> int:
    tot = 0
    for dct in some_list:
        for key, val in dct.items():
            tot += key.count(target)
            if isinstance(val, str):
                tot += val.count(target)
            if stop_after is not None and tot >= stop_after:
                return tot

    return tot


def keygen(base_str: str, base_ct=0) -> Generator[str, None, None]:
    bs = base_str
    count = 0
    while True:
        yield f"${{{bs}{count}}}"
        count += 1


def build_word_map(starting_list: DictList, key_gen: Generator[str, None, None],
                   selector: str, typical_tok_len=6) -> OrderedDict[str, str]:
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


def build_char_map(starting_list: DictList, key_gen: Generator[str, None, None],
                   selector: str , typical_tok_len=6) -> OrderedDict[str, str]:
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
    best_dct = {}
    for wt, path_str, k, lk in sort_me:
        best_dct[path_str] = (wt, k, lk)
    char_subst_dict = OrderedDict()
    next_tok = next(key_gen)
    for path_str, (wt, k, lk) in best_dct.items():
        if wt > 0:
            subst_str = path_str
            if len(subst_str) > len(next_tok):
                char_subst_dict[subst_str] = next_tok
                next_tok = next(key_gen)
    return char_subst_dict


def build_reverse_char_map(starting_list: DictList, key_gen: Generator[str, None, None],
                           selector: str, typical_tok_len=6) -> OrderedDict[str, str]:
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


def fully_template(st: str, dct: Dict[str, str]) -> str:
    prev_st = None
    while st != prev_st:
        prev_st = st
        st = Template(st).substitute(dct)
    return st


def fully_template_dict_list(dict_list: list[Dict[str, str]],
                             template_dict: Dict[str, str]) -> list[Dict[str, str]]:
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


def full_map(dict_list: list[Dict[str, str]], mapper: MappingFunc,
             key_gen: Generator[str, None, None],
             sel: str, typical_tok_len=6, verbose=False) -> OrderedDict[str, str]:
    best_total_len = total_len(dict_list)
    itr = 0
    cum_subst_d = OrderedDict()
    while True:
        subst_d = mapper(dict_list, key_gen, sel, typical_tok_len=typical_tok_len)
        for key in subst_d:
            if count_occurrences(dict_list, key) > 1:
                cum_subst_d[key] = subst_d[key]
        dict_list = apply_substitutions(dict_list, cum_subst_d)
        new_total_len = total_len(dict_list) + total_len([cum_subst_d])
        if ((new_total_len >= best_total_len - typical_tok_len)
            or itr > 100):
            break
        else:
            best_total_len = new_total_len
            itr += 1
    if verbose:
        print(f"final total length for {sel}: {best_total_len} with {len(cum_subst_d)} substitutions")
    return cum_subst_d


def invert_and_clean_subst_dict(subst_d: OrderedDict[str, str]) -> dict[str, str]:
    inv_subst_d = OrderedDict((val[2:-1], key) 
                              for key, val in subst_d.items())
    clean_d = {}
    for key, val in inv_subst_d.items():
        clean_d[key] = fully_template(val, inv_subst_d)
    return clean_d


def generate_files_template(dict_list: DictList, verbose=False) -> tuple(dict[str, str], DictList):
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
                             typical_tok_len=typical_tok_len,
                             verbose=verbose)
    files_l = apply_substitutions(files_l, descr_subst_d)
    substitution_dict.update(descr_subst_d)

    term_subst_d = full_map(files_l,
                            build_char_map,
                            keygen("t"),
                            "edam_term",
                            typical_tok_len=typical_tok_len,
                            verbose=verbose)
    files_l = apply_substitutions(files_l, term_subst_d)
    substitution_dict.update(term_subst_d)

    path_subst_d = full_map(files_l,
                            build_reverse_char_map,
                            keygen("p"),
                            "rel_path",
                            typical_tok_len=typical_tok_len,
                            verbose=verbose)
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
    key_dict["unknown"] = "${k5}"
    files_l = apply_substitutions(files_l, key_dict, verbose=False)
    substitution_dict.update(key_dict)

    clean_d = invert_and_clean_subst_dict(substitution_dict)

    return clean_d, files_l


class TemplateBuilder:
    def __init__(self, dict_list: DictList, verbose=False):
        self.dict_list = dict_list
        self.subst_dict, self.templated_list = generate_files_template(dict_list, verbose=verbose)
    
    def apply(self):
        return [self.subst_dict] + self.templated_list
    
    @staticmethod
    def expand(dict_list: DictList) -> DictList:
        candidate_subst_d = dict_list[0]
        if not isinstance(candidate_subst_d, dict):
            raise ValueError("First element of dict_list must be a dictionary")
        elif "k0" not in candidate_subst_d or "description" in candidate_subst_d:
            # This dict list is not templated, so we can just return it as is
            return dict_list
        else:
            return fully_template_dict_list(dict_list[1:], candidate_subst_d)
