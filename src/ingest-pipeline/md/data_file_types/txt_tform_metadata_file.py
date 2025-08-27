#! /usr/bin/env python

from metadata_file import MetadataFile

class TxtTformMetadataFile(MetadataFile):
    """A metadata file type for files containing geometrical transforms as text"""
    category_name = 'TxtTform'

    def collect_metadata(self):
        print('parsing transformation text from %s' % self.path)
        rslt = {}
        with open(self.path, 'r') as f:
            for line in f:
                line = line.strip()
                assert line[0] == '(' and line[-1] == ')', "Missing parens line <{}>".format(line)
                line = line[1:-1]
                words = line.split()
                typed_words = []
                for word in words[1:]:
                    if word == '"true"':
                        word = True
                    elif word == '"false"':
                        word = False
                    elif word[0] == '"' and word[-1] == '"':
                        word = word[1:-1]
                    else:
                        try:
                            word = int(word)
                        except ValueError:
                            try:
                                word = float(word)
                            except ValueError:
                                pass
                    typed_words.append(word)
                assert typed_words , "Unexpected format line <{}>".format(line)
                if len(typed_words) == 1:
                    rslt[words[0]] = typed_words[0]
                else:
                    rslt[words[0]] = typed_words
        return rslt
