#! /usr/bin/env python

from metadata_file import MetadataFile
import frontmatter

class MarkdownMetadataFile(MetadataFile):
    """A metadata file type for markdown files with yaml frontmatter """
    category_name = 'MD';

    def collect_metadata(self):
        print('parsing yaml and markdown from %s' % self.path)
        
        with open(self.path, 'rU') as f:
            post = frontmatter.load(f)
            print(post.keys())
            print(f'content follows: <{post.content}>')
        md = {}
        for key in post.keys(): # awkward but apparently needed
            md[key] = post[key]
        md['content'] = post.content
        return md
