# -*- coding: utf-8 -*-
#
# Documentation config
#

import sys, os, datetime

sys.path.append(os.path.abspath('exts'))
sys.path.append(os.path.abspath('utils'))

import sbt_versions

highlight_language = 'text'
extensions = ['sphinx.ext.extlinks', 'includecode']
templates_path = ['_templates']
source_suffix = '.rst'
master_doc = 'index'
exclude_patterns = ['metrics/*', 'shared-modules/*']

sys.path.append(os.path.abspath('_themes'))
html_theme_path = ['_themes']
html_theme = 'flask'
html_short_title = 'Finagle'
html_static_path = ['_static']
html_sidebars = {
   'index':    ['sidebarintro.html', 'searchbox.html'],
   '**':       ['sidebarlogo.html', 'sidebarintro.html', 'localtoc.html', 'relations.html',
                'searchbox.html']
}
html_favicon = '_static/favicon.ico'
html_theme_options = {
  'index_logo': None
}

html_show_sphinx = False
html_style = 'finagle.css'

project = 'Finagle'
copyright = '{} Twitter, Inc'.format(datetime.datetime.now().year)
htmlhelp_basename = "finagle"
release = sbt_versions.find_release(os.path.abspath('../../../project/Build.scala'))
version = sbt_versions.release_to_version(release)

# e.g. :issue:`36` :ticket:`8`
# or :src:`BufferingPool <com/twitter/finagle/pool/BufferingPool.scala>`
extlinks = {
  'issue': ('https://github.com/twitter/finagle/issues/%s', 'issue #'),
  'ex': ('https://github.com/twitter/finagle/blob/finagle-example/src/main/scala/%s', 'Finagle example '),
  'api': ('https://twitter.github.io/finagle/docs/%s', ''),
  'util': ('https://twitter.github.io/util/docs/%s', ''),
  'util-app-src': ("https://github.com/twitter/util/blob/release/util-app/src/main/scala/%s", 'util-app github repo'),
  'util-core-src': ("https://github.com/twitter/util/blob/release/util-core/src/main/scala/%s", 'util-core github repo'),
  'util-stats-src': ("https://github.com/twitter/util/blob/release/util-stats/src/main/scala/%s", 'util-stats github repo'),
  'util-tunable-src': ("https://github.com/twitter/util/blob/release/util-tunable/src/main/scala/%s", 'util-tunable github repo'),
  'finagle-http-src': ("https://github.com/twitter/finagle/blob/release/finagle-http/src/main/scala/%s", 'finagle-http github repo'),
  'finagle-base-http-src': ("https://github.com/twitter/finagle/blob/release/finagle-base-http/src/main/scala/%s", 'finagle-base-http github repo'),
  'finagle-netty4-src': ("https://github.com/twitter/finagle/blob/release/finagle-netty4/src/main/scala/%s", 'finagle-netty4 github repo'),
  'finagle-mux-src': ("https://github.com/twitter/finagle/blob/release/finagle-mux/src/main/scala/%s", 'finagle-mux github repo'),
  'finagle-thrift-src': ("https://github.com/twitter/finagle/blob/release/finagle-thrift/src/main/scala/%s", 'finagle-thrift github repo'),
  'finagle-thrift-test': ("https://github.com/twitter/finagle/blob/release/finagle-thrift/src/test/scala/%s", 'finagle-thrift github tests'),
  'finagle-thriftmux-src': ("https://github.com/twitter/finagle/blob/release/finagle-thriftmux/src/main/scala/%s", 'finagle-thriftmux github repo'),
  'finagle-toggle-src': ("https://github.com/twitter/finagle/blob/release/finagle-toggle/src/main/scala/%s", 'finagle-toggle github repo'),
  'finagle-tunable-src': ("https://github.com/twitter/finagle/blob/release/finagle-tunable/src/main/scala/%s", 'finagle-tunable github repo'),
  'src': ("https://github.com/twitter/finagle/blob/release/finagle-core/src/main/scala/%s", 'finagle-core github repo')
}

rst_epilog = '''
.. _Finagle Examples: https://github.com/twitter/finagle/tree/finagle-example
'''

pygments_style = 'flask_theme_support.FlaskyStyle'

# fall back if theme is not there
try:
    __import__('flask_theme_support')
except ImportError as e:
    print('-' * 74)
    print('Warning: Flask themes unavailable.  Building with default theme')
    print('If you want the Flask themes, run this command and build again:')
    print()
    print('  git submodule update --init')
    print('-' * 74)

    pygments_style = 'tango'
    html_theme = 'default'
    html_theme_options = {}
