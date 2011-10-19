


<!DOCTYPE html>
<html>
  <head>
    <meta charset='utf-8'>
    <meta http-equiv="X-UA-Compatible" content="chrome=1">
        <title>finagle-core/src/main/scala/com/twitter/finagle/Service.scala at master from mccue/finagle - GitHub</title>
    <link rel="search" type="application/opensearchdescription+xml" href="/opensearch.xml" title="GitHub" />
    <link rel="fluid-icon" href="https://github.com/fluidicon.png" title="GitHub" />

    
    

    <meta content="authenticity_token" name="csrf-param" />
<meta content="5473a6c80967c6cfeed5c004c546a1dbc0630d0f" name="csrf-token" />

    <link href="https://a248.e.akamai.net/assets.github.com/stylesheets/bundle_github.css?274c62b0fa5396961fbd444ea6b1ca3feb9e204f" media="screen" rel="stylesheet" type="text/css" />
    

    <script src="https://a248.e.akamai.net/assets.github.com/javascripts/bundle_jquery.js?feac705db7bad7450550f9b531ff2d76f0ce30c4" type="text/javascript"></script>

    <script src="https://a248.e.akamai.net/assets.github.com/javascripts/bundle_github.js?7267540807bb3adb6133264d22481c6bff644121" type="text/javascript"></script>

    

      <link rel='permalink' href='/mccue/finagle/blob/338669002c4a707936b65400a24e851482cc5977/finagle-core/src/main/scala/com/twitter/finagle/Service.scala'>
    <META NAME="ROBOTS" CONTENT="NOINDEX, FOLLOW">

    <meta name="description" content="finagle - A fault tolerant, protocol-agnostic RPC system" />
  <link href="https://github.com/mccue/finagle/commits/master.atom" rel="alternate" title="Recent Commits to finagle:master" type="application/atom+xml" />

  </head>


  <body class="logged_in page-blob  env-production ">
    

    


    

    <div id="main">
      <div id="header" class="true">
          <a class="logo" href="https://github.com/">
            <img alt="github" class="default svg" height="45" src="https://a248.e.akamai.net/assets.github.com/images/modules/header/logov6.svg" />
            <img alt="github" class="default png" height="45" src="https://a248.e.akamai.net/assets.github.com/images/modules/header/logov6.png" />
            <!--[if (gt IE 8)|!(IE)]><!-->
            <img alt="github" class="hover svg" height="45" src="https://a248.e.akamai.net/assets.github.com/images/modules/header/logov6-hover.svg" />
            <img alt="github" class="hover png" height="45" src="https://a248.e.akamai.net/assets.github.com/images/modules/header/logov6-hover.png" />
            <!--<![endif]-->
          </a>

          


    <div class="userbox">
      <div class="avatarname">
        <a href="https://github.com/mccue"><img src="https://secure.gravatar.com/avatar/af3e1fc086aaed49a0eff11a0ea266b1?s=140&d=https://a248.e.akamai.net/assets.github.com%2Fimages%2Fgravatars%2Fgravatar-140.png" alt="" width="20" height="20"  /></a>
        <a href="https://github.com/mccue" class="name">mccue</a>

      </div>
      <ul class="usernav">
        <li><a href="https://github.com/">Dashboard</a></li>
        <li>
          <a href="https://github.com/inbox">Inbox</a>
          <a href="https://github.com/inbox" class="unread_count ">0</a>
        </li>
        <li><a href="https://github.com/account">Account Settings</a></li>
        <li><a href="/logout">Log Out</a></li>
      </ul>
    </div><!-- /.userbox -->


        <div class="topsearch">
<form action="/search" id="top_search_form" method="get">      <a href="/search" class="advanced-search tooltipped downwards" title="Advanced Search">Advanced Search</a>
      <div class="search placeholder-field js-placeholder-field">
        <label class="placeholder" for="global-search-field">Search…</label>
        <input type="text" class="search my_repos_autocompleter" id="global-search-field" name="q" results="5" /> <input type="submit" value="Search" class="button" />
      </div>
      <input type="hidden" name="type" value="Everything" />
      <input type="hidden" name="repo" value="" />
      <input type="hidden" name="langOverride" value="" />
      <input type="hidden" name="start_value" value="1" />
</form>    <ul class="nav">
        <li class="explore"><a href="https://github.com/explore">Explore GitHub</a></li>
        <li><a href="https://gist.github.com">Gist</a></li>
        <li><a href="/blog">Blog</a></li>
      <li><a href="http://help.github.com">Help</a></li>
    </ul>
</div>

      </div>

      
            <div class="site">
      <div class="pagehead repohead vis-public fork  instapaper_ignore readability-menu">


      <div class="title-actions-bar">
        <h1>
          <a href="/mccue">mccue</a> /
          <strong><a href="/mccue/finagle" class="js-current-repository">finagle</a></strong>
            <span class="fork-flag">
              <span class="text">forked from <a href="/twitter/finagle">twitter/finagle</a></span>
            </span>
        </h1>
        



            <ul class="pagehead-actions">

          <li class="for-owner"><a href="/mccue/finagle/admin" class="minibutton btn-admin "><span><span class="icon"></span>Admin</span></a></li>
        <li>
            <a href="/mccue/finagle/toggle_watch" class="minibutton btn-watch unwatch-button" onclick="var f = document.createElement('form'); f.style.display = 'none'; this.parentNode.appendChild(f); f.method = 'POST'; f.action = this.href;var s = document.createElement('input'); s.setAttribute('type', 'hidden'); s.setAttribute('name', 'authenticity_token'); s.setAttribute('value', '5473a6c80967c6cfeed5c004c546a1dbc0630d0f'); f.appendChild(s);f.submit();return false;"><span><span class="icon"></span>Unwatch</span></a>
        </li>
            <li><a href="/mccue/finagle/fork" class="minibutton btn-fork fork-button" onclick="var f = document.createElement('form'); f.style.display = 'none'; this.parentNode.appendChild(f); f.method = 'POST'; f.action = this.href;var s = document.createElement('input'); s.setAttribute('type', 'hidden'); s.setAttribute('name', 'authenticity_token'); s.setAttribute('value', '5473a6c80967c6cfeed5c004c546a1dbc0630d0f'); f.appendChild(s);f.submit();return false;"><span><span class="icon"></span>Fork</span></a></li>

          <li class='nspr'><a href="/mccue/finagle/pull/new/master" class="minibutton btn-pull-request "><span><span class="icon"></span>Pull Request</span></a></li>
      <li class="repostats">
        <ul class="repo-stats">
          <li class="watchers watching">
            <a href="/mccue/finagle/watchers" title="Watchers — You're Watching" class="tooltipped downwards">
              1
            </a>
          </li>
          <li class="forks">
            <a href="/mccue/finagle/network" title="Forks" class="tooltipped downwards">
              31
            </a>
          </li>
        </ul>
      </li>
    </ul>

      </div>

        

  <ul class="tabs">
    <li><a href="/mccue/finagle" class="selected" highlight="repo_sourcerepo_downloadsrepo_commitsrepo_tagsrepo_branches">Code</a></li>
    <li><a href="/mccue/finagle/network" highlight="repo_networkrepo_fork_queue">Network</a>
    <li><a href="/mccue/finagle/pulls" highlight="repo_pulls">Pull Requests <span class='counter'>0</span></a></li>


      <li><a href="/mccue/finagle/wiki" highlight="repo_wiki">Wiki <span class='counter'>0</span></a></li>

    <li><a href="/mccue/finagle/graphs" highlight="repo_graphsrepo_contributors">Stats &amp; Graphs</a></li>

  </ul>

  

<div class="subnav-bar">

  <ul class="actions">
    
      <li class="switcher">

        <div class="context-menu-container js-menu-container">
          <span class="text">Current branch:</span>
          <a href="#"
             class="minibutton bigger switcher context-menu-button js-menu-target js-commitish-button btn-branch repo-tree"
             data-master-branch="master"
             data-ref="master">
            <span><span class="icon"></span>master</span>
          </a>

          <div class="context-pane commitish-context js-menu-content">
            <a href="javascript:;" class="close js-menu-close"></a>
            <div class="title">Switch Branches/Tags</div>
            <div class="body pane-selector commitish-selector js-filterable-commitishes">
              <div class="filterbar">
                <div class="placeholder-field js-placeholder-field">
                  <label class="placeholder" for="context-commitish-filter-field" data-placeholder-mode="sticky">Filter branches/tags</label>
                  <input type="text" id="context-commitish-filter-field" class="commitish-filter" />
                </div>

                <ul class="tabs">
                  <li><a href="#" data-filter="branches" class="selected">Branches</a></li>
                  <li><a href="#" data-filter="tags">Tags</a></li>
                </ul>
              </div>

                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/b3_compatible_tracing/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="b3_compatible_tracing">b3_compatible_tracing</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/copy_channel_buffer_after_decode_line/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="copy_channel_buffer_after_decode_line">copy_channel_buffer_after_decode_line</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/finagle-power-pack-2000/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="finagle-power-pack-2000">finagle-power-pack-2000</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/finagle_1_0_x/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="finagle_1_0_x">finagle_1_0_x</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/fix_race_in_service_to_channel/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="fix_race_in_service_to_channel">fix_race_in_service_to_channel</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/gh-pages/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="gh-pages">gh-pages</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/gzip_ssl/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="gzip_ssl">gzip_ssl</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/kestrel/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="kestrel">kestrel</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/master/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="master">master</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/memcache-long/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="memcache-long">memcache-long</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/native/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="native">native</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/netty/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="netty">netty</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/new_ostrich/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="new_ostrich">new_ostrich</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/ostrich4/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="ostrich4">ostrich4</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/ostrich_3/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="ostrich_3">ostrich_3</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/retry_strategies/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="retry_strategies">retry_strategies</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/serialization/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="serialization">serialization</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/ssl_track_cipher/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="ssl_track_cipher">ssl_track_cipher</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/stats_refactoring/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="stats_refactoring">stats_refactoring</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/svc/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="svc">svc</a>
                  </h4>
                </div>
                <div class="commitish-item branch-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/thrift_client_reply_refactor/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="thrift_client_reply_refactor">thrift_client_reply_refactor</a>
                  </h4>
                </div>

                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.2.3/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.2.3">version-1.2.3</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.2.2/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.2.2">version-1.2.2</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.32/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.32">version-1.1.32</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.30/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.30">version-1.1.30</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.29/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.29">version-1.1.29</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.28/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.28">version-1.1.28</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.27/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.27">version-1.1.27</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.26/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.26">version-1.1.26</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.24/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.24">version-1.1.24</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.18/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.18">version-1.1.18</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.17/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.17">version-1.1.17</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.15/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.15">version-1.1.15</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.14/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.14">version-1.1.14</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.13/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.13">version-1.1.13</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.12/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.12">version-1.1.12</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.11/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.11">version-1.1.11</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.10/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.10">version-1.1.10</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.9/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.9">version-1.1.9</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.8/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.8">version-1.1.8</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.7/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.7">version-1.1.7</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.4/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.4">version-1.1.4</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.3/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.3">version-1.1.3</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.2/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.2">version-1.1.2</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.1.1/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.1.1">version-1.1.1</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.0.21/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.0.21">version-1.0.21</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.0.20/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.0.20">version-1.0.20</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.0.19/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.0.19">version-1.0.19</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.0.16/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.0.16">version-1.0.16</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.0.15/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.0.15">version-1.0.15</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.0.12/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.0.12">version-1.0.12</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.0.11/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.0.11">version-1.0.11</a>
                  </h4>
                </div>
                <div class="commitish-item tag-commitish selector-item">
                  <h4>
                      <a href="/mccue/finagle/blob/version-1.0.9/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-name="version-1.0.9">version-1.0.9</a>
                  </h4>
                </div>

              <div class="no-results" style="display:none">Nothing to show</div>
            </div>
          </div><!-- /.commitish-context-context -->
        </div>

      </li>
  </ul>

  <ul class="subnav">
    <li><a href="/mccue/finagle" class="selected" highlight="repo_source">Files</a></li>
    <li><a href="/mccue/finagle/commits/master" highlight="repo_commits">Commits</a></li>
    <li><a href="/mccue/finagle/branches" class="" highlight="repo_branches">Branches <span class="counter">21</span></a></li>
    <li><a href="/mccue/finagle/tags" class="" highlight="repo_tags">Tags <span class="counter">32</span></a></li>
    <li><a href="/mccue/finagle/downloads" class="blank" highlight="repo_downloads">Downloads <span class="counter">0</span></a></li>
  </ul>

</div>

  
  
  


        

      </div><!-- /.pagehead -->

      




  
  <p class="last-commit">Latest commit to the <strong>master</strong> branch</p>

<div class="commit commit-tease js-details-container">
  <p class="commit-title ">
      <a href="/mccue/finagle"><a href="/mccue/finagle/commit/338669002c4a707936b65400a24e851482cc5977" class="message">[split] Merge commit 'a8f0b21a582c9198f7b9a493da1fa0eb53cf257a'</a></a>
      
  </p>
  <div class="commit-meta">
    <a href="/mccue/finagle/commit/338669002c4a707936b65400a24e851482cc5977" class="sha-block">commit <span class="sha">338669002c</span></a>

    <div class="authorship">
      <img src="https://secure.gravatar.com/avatar/0be8dfe28d138dcd76466b47df2fb8d0?s=140&d=https://a248.e.akamai.net/assets.github.com%2Fimages%2Fgravatars%2Fgravatar-140.png" alt="" width="20" height="20" class="gravatar" />
      <span class="author-name"><a href="/mariusaeriksen">mariusaeriksen</a></span>
      authored <time class="js-relative-date" datetime="2011-10-11T16:16:24-07:00" title="2011-10-11 16:16:24">October 11, 2011</time>

    </div>
  </div>
</div>


  <div id="slider">

    <div class="breadcrumb" data-path="finagle-core/src/main/scala/com/twitter/finagle/Service.scala/">
      <b><a href="/mccue/finagle/tree/338669002c4a707936b65400a24e851482cc5977" class="js-rewrite-sha">finagle</a></b> / <a href="/mccue/finagle/tree/338669002c4a707936b65400a24e851482cc5977/finagle-core" class="js-rewrite-sha">finagle-core</a> / <a href="/mccue/finagle/tree/338669002c4a707936b65400a24e851482cc5977/finagle-core/src" class="js-rewrite-sha">src</a> / <a href="/mccue/finagle/tree/338669002c4a707936b65400a24e851482cc5977/finagle-core/src/main" class="js-rewrite-sha">main</a> / <a href="/mccue/finagle/tree/338669002c4a707936b65400a24e851482cc5977/finagle-core/src/main/scala" class="js-rewrite-sha">scala</a> / <a href="/mccue/finagle/tree/338669002c4a707936b65400a24e851482cc5977/finagle-core/src/main/scala/com" class="js-rewrite-sha">com</a> / <a href="/mccue/finagle/tree/338669002c4a707936b65400a24e851482cc5977/finagle-core/src/main/scala/com/twitter" class="js-rewrite-sha">twitter</a> / <a href="/mccue/finagle/tree/338669002c4a707936b65400a24e851482cc5977/finagle-core/src/main/scala/com/twitter/finagle" class="js-rewrite-sha">finagle</a> / Service.scala       <span style="display:none" id="clippy_1073" class="clippy-text">finagle-core/src/main/scala/com/twitter/finagle/Service.scala</span>
      
      <object classid="clsid:d27cdb6e-ae6d-11cf-96b8-444553540000"
              width="110"
              height="14"
              class="clippy"
              id="clippy" >
      <param name="movie" value="https://a248.e.akamai.net/assets.github.com/flash/clippy.swf?v5"/>
      <param name="allowScriptAccess" value="always" />
      <param name="quality" value="high" />
      <param name="scale" value="noscale" />
      <param NAME="FlashVars" value="id=clippy_1073&amp;copied=copied!&amp;copyto=copy to clipboard">
      <param name="bgcolor" value="#FFFFFF">
      <param name="wmode" value="opaque">
      <embed src="https://a248.e.akamai.net/assets.github.com/flash/clippy.swf?v5"
             width="110"
             height="14"
             name="clippy"
             quality="high"
             allowScriptAccess="always"
             type="application/x-shockwave-flash"
             pluginspage="http://www.macromedia.com/go/getflashplayer"
             FlashVars="id=clippy_1073&amp;copied=copied!&amp;copyto=copy to clipboard"
             bgcolor="#FFFFFF"
             wmode="opaque"
      />
      </object>
      

    </div>

    <div class="frames">
      <div class="frame frame-center" data-path="finagle-core/src/main/scala/com/twitter/finagle/Service.scala/" data-permalink-url="/mccue/finagle/blob/338669002c4a707936b65400a24e851482cc5977/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-title="finagle-core/src/main/scala/com/twitter/finagle/Service.scala at master from mccue/finagle - GitHub" data-type="blob">
          <ul class="big-actions">
            <li><a class="file-edit-link minibutton js-rewrite-sha" href="/mccue/finagle/edit/338669002c4a707936b65400a24e851482cc5977/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" data-method="post"><span>Edit this file</span></a></li>
          </ul>

        <div id="files">
          <div class="file">
            <div class="meta">
              <div class="info">
                <span class="icon"><img alt="Txt" height="16" src="https://a248.e.akamai.net/assets.github.com/images/icons/txt.png" width="16" /></span>
                <span class="mode" title="File Mode">100644</span>
                  <span>218 lines (193 sloc)</span>
                <span>6.783 kb</span>
              </div>
              <ul class="actions">
                <li><a href="/mccue/finagle/raw/master/finagle-core/src/main/scala/com/twitter/finagle/Service.scala" id="raw-url">raw</a></li>
                  <li><a href="/mccue/finagle/blame/master/finagle-core/src/main/scala/com/twitter/finagle/Service.scala">blame</a></li>
                <li><a href="/mccue/finagle/commits/master/finagle-core/src/main/scala/com/twitter/finagle/Service.scala">history</a></li>
              </ul>
            </div>
              <div class="data type-scala">
      <table cellpadding="0" cellspacing="0" class="lines">
        <tr>
          <td>
            <pre class="line_numbers"><span id="L1" rel="#L1">1</span>
<span id="L2" rel="#L2">2</span>
<span id="L3" rel="#L3">3</span>
<span id="L4" rel="#L4">4</span>
<span id="L5" rel="#L5">5</span>
<span id="L6" rel="#L6">6</span>
<span id="L7" rel="#L7">7</span>
<span id="L8" rel="#L8">8</span>
<span id="L9" rel="#L9">9</span>
<span id="L10" rel="#L10">10</span>
<span id="L11" rel="#L11">11</span>
<span id="L12" rel="#L12">12</span>
<span id="L13" rel="#L13">13</span>
<span id="L14" rel="#L14">14</span>
<span id="L15" rel="#L15">15</span>
<span id="L16" rel="#L16">16</span>
<span id="L17" rel="#L17">17</span>
<span id="L18" rel="#L18">18</span>
<span id="L19" rel="#L19">19</span>
<span id="L20" rel="#L20">20</span>
<span id="L21" rel="#L21">21</span>
<span id="L22" rel="#L22">22</span>
<span id="L23" rel="#L23">23</span>
<span id="L24" rel="#L24">24</span>
<span id="L25" rel="#L25">25</span>
<span id="L26" rel="#L26">26</span>
<span id="L27" rel="#L27">27</span>
<span id="L28" rel="#L28">28</span>
<span id="L29" rel="#L29">29</span>
<span id="L30" rel="#L30">30</span>
<span id="L31" rel="#L31">31</span>
<span id="L32" rel="#L32">32</span>
<span id="L33" rel="#L33">33</span>
<span id="L34" rel="#L34">34</span>
<span id="L35" rel="#L35">35</span>
<span id="L36" rel="#L36">36</span>
<span id="L37" rel="#L37">37</span>
<span id="L38" rel="#L38">38</span>
<span id="L39" rel="#L39">39</span>
<span id="L40" rel="#L40">40</span>
<span id="L41" rel="#L41">41</span>
<span id="L42" rel="#L42">42</span>
<span id="L43" rel="#L43">43</span>
<span id="L44" rel="#L44">44</span>
<span id="L45" rel="#L45">45</span>
<span id="L46" rel="#L46">46</span>
<span id="L47" rel="#L47">47</span>
<span id="L48" rel="#L48">48</span>
<span id="L49" rel="#L49">49</span>
<span id="L50" rel="#L50">50</span>
<span id="L51" rel="#L51">51</span>
<span id="L52" rel="#L52">52</span>
<span id="L53" rel="#L53">53</span>
<span id="L54" rel="#L54">54</span>
<span id="L55" rel="#L55">55</span>
<span id="L56" rel="#L56">56</span>
<span id="L57" rel="#L57">57</span>
<span id="L58" rel="#L58">58</span>
<span id="L59" rel="#L59">59</span>
<span id="L60" rel="#L60">60</span>
<span id="L61" rel="#L61">61</span>
<span id="L62" rel="#L62">62</span>
<span id="L63" rel="#L63">63</span>
<span id="L64" rel="#L64">64</span>
<span id="L65" rel="#L65">65</span>
<span id="L66" rel="#L66">66</span>
<span id="L67" rel="#L67">67</span>
<span id="L68" rel="#L68">68</span>
<span id="L69" rel="#L69">69</span>
<span id="L70" rel="#L70">70</span>
<span id="L71" rel="#L71">71</span>
<span id="L72" rel="#L72">72</span>
<span id="L73" rel="#L73">73</span>
<span id="L74" rel="#L74">74</span>
<span id="L75" rel="#L75">75</span>
<span id="L76" rel="#L76">76</span>
<span id="L77" rel="#L77">77</span>
<span id="L78" rel="#L78">78</span>
<span id="L79" rel="#L79">79</span>
<span id="L80" rel="#L80">80</span>
<span id="L81" rel="#L81">81</span>
<span id="L82" rel="#L82">82</span>
<span id="L83" rel="#L83">83</span>
<span id="L84" rel="#L84">84</span>
<span id="L85" rel="#L85">85</span>
<span id="L86" rel="#L86">86</span>
<span id="L87" rel="#L87">87</span>
<span id="L88" rel="#L88">88</span>
<span id="L89" rel="#L89">89</span>
<span id="L90" rel="#L90">90</span>
<span id="L91" rel="#L91">91</span>
<span id="L92" rel="#L92">92</span>
<span id="L93" rel="#L93">93</span>
<span id="L94" rel="#L94">94</span>
<span id="L95" rel="#L95">95</span>
<span id="L96" rel="#L96">96</span>
<span id="L97" rel="#L97">97</span>
<span id="L98" rel="#L98">98</span>
<span id="L99" rel="#L99">99</span>
<span id="L100" rel="#L100">100</span>
<span id="L101" rel="#L101">101</span>
<span id="L102" rel="#L102">102</span>
<span id="L103" rel="#L103">103</span>
<span id="L104" rel="#L104">104</span>
<span id="L105" rel="#L105">105</span>
<span id="L106" rel="#L106">106</span>
<span id="L107" rel="#L107">107</span>
<span id="L108" rel="#L108">108</span>
<span id="L109" rel="#L109">109</span>
<span id="L110" rel="#L110">110</span>
<span id="L111" rel="#L111">111</span>
<span id="L112" rel="#L112">112</span>
<span id="L113" rel="#L113">113</span>
<span id="L114" rel="#L114">114</span>
<span id="L115" rel="#L115">115</span>
<span id="L116" rel="#L116">116</span>
<span id="L117" rel="#L117">117</span>
<span id="L118" rel="#L118">118</span>
<span id="L119" rel="#L119">119</span>
<span id="L120" rel="#L120">120</span>
<span id="L121" rel="#L121">121</span>
<span id="L122" rel="#L122">122</span>
<span id="L123" rel="#L123">123</span>
<span id="L124" rel="#L124">124</span>
<span id="L125" rel="#L125">125</span>
<span id="L126" rel="#L126">126</span>
<span id="L127" rel="#L127">127</span>
<span id="L128" rel="#L128">128</span>
<span id="L129" rel="#L129">129</span>
<span id="L130" rel="#L130">130</span>
<span id="L131" rel="#L131">131</span>
<span id="L132" rel="#L132">132</span>
<span id="L133" rel="#L133">133</span>
<span id="L134" rel="#L134">134</span>
<span id="L135" rel="#L135">135</span>
<span id="L136" rel="#L136">136</span>
<span id="L137" rel="#L137">137</span>
<span id="L138" rel="#L138">138</span>
<span id="L139" rel="#L139">139</span>
<span id="L140" rel="#L140">140</span>
<span id="L141" rel="#L141">141</span>
<span id="L142" rel="#L142">142</span>
<span id="L143" rel="#L143">143</span>
<span id="L144" rel="#L144">144</span>
<span id="L145" rel="#L145">145</span>
<span id="L146" rel="#L146">146</span>
<span id="L147" rel="#L147">147</span>
<span id="L148" rel="#L148">148</span>
<span id="L149" rel="#L149">149</span>
<span id="L150" rel="#L150">150</span>
<span id="L151" rel="#L151">151</span>
<span id="L152" rel="#L152">152</span>
<span id="L153" rel="#L153">153</span>
<span id="L154" rel="#L154">154</span>
<span id="L155" rel="#L155">155</span>
<span id="L156" rel="#L156">156</span>
<span id="L157" rel="#L157">157</span>
<span id="L158" rel="#L158">158</span>
<span id="L159" rel="#L159">159</span>
<span id="L160" rel="#L160">160</span>
<span id="L161" rel="#L161">161</span>
<span id="L162" rel="#L162">162</span>
<span id="L163" rel="#L163">163</span>
<span id="L164" rel="#L164">164</span>
<span id="L165" rel="#L165">165</span>
<span id="L166" rel="#L166">166</span>
<span id="L167" rel="#L167">167</span>
<span id="L168" rel="#L168">168</span>
<span id="L169" rel="#L169">169</span>
<span id="L170" rel="#L170">170</span>
<span id="L171" rel="#L171">171</span>
<span id="L172" rel="#L172">172</span>
<span id="L173" rel="#L173">173</span>
<span id="L174" rel="#L174">174</span>
<span id="L175" rel="#L175">175</span>
<span id="L176" rel="#L176">176</span>
<span id="L177" rel="#L177">177</span>
<span id="L178" rel="#L178">178</span>
<span id="L179" rel="#L179">179</span>
<span id="L180" rel="#L180">180</span>
<span id="L181" rel="#L181">181</span>
<span id="L182" rel="#L182">182</span>
<span id="L183" rel="#L183">183</span>
<span id="L184" rel="#L184">184</span>
<span id="L185" rel="#L185">185</span>
<span id="L186" rel="#L186">186</span>
<span id="L187" rel="#L187">187</span>
<span id="L188" rel="#L188">188</span>
<span id="L189" rel="#L189">189</span>
<span id="L190" rel="#L190">190</span>
<span id="L191" rel="#L191">191</span>
<span id="L192" rel="#L192">192</span>
<span id="L193" rel="#L193">193</span>
<span id="L194" rel="#L194">194</span>
<span id="L195" rel="#L195">195</span>
<span id="L196" rel="#L196">196</span>
<span id="L197" rel="#L197">197</span>
<span id="L198" rel="#L198">198</span>
<span id="L199" rel="#L199">199</span>
<span id="L200" rel="#L200">200</span>
<span id="L201" rel="#L201">201</span>
<span id="L202" rel="#L202">202</span>
<span id="L203" rel="#L203">203</span>
<span id="L204" rel="#L204">204</span>
<span id="L205" rel="#L205">205</span>
<span id="L206" rel="#L206">206</span>
<span id="L207" rel="#L207">207</span>
<span id="L208" rel="#L208">208</span>
<span id="L209" rel="#L209">209</span>
<span id="L210" rel="#L210">210</span>
<span id="L211" rel="#L211">211</span>
<span id="L212" rel="#L212">212</span>
<span id="L213" rel="#L213">213</span>
<span id="L214" rel="#L214">214</span>
<span id="L215" rel="#L215">215</span>
<span id="L216" rel="#L216">216</span>
<span id="L217" rel="#L217">217</span>
<span id="L218" rel="#L218">218</span>
</pre>
          </td>
          <td width="100%">
                <div class="highlight"><pre><div class='line' id='LC1'><span class="k">package</span> <span class="nn">com.twitter.finagle</span></div><div class='line' id='LC2'><br/></div><div class='line' id='LC3'><span class="k">import</span> <span class="nn">java.net.SocketAddress</span></div><div class='line' id='LC4'><span class="k">import</span> <span class="nn">com.twitter.finagle.service.RefcountedService</span></div><div class='line' id='LC5'><span class="k">import</span> <span class="nn">com.twitter.util.Future</span></div><div class='line' id='LC6'><span class="k">import</span> <span class="nn">org.jboss.netty.channel.Channel</span></div><div class='line' id='LC7'><br/></div><div class='line' id='LC8'><span class="cm">/**</span></div><div class='line' id='LC9'><span class="cm"> * A Service is an asynchronous function from Request to Future[Response]. It is the</span></div><div class='line' id='LC10'><span class="cm"> * basic unit of an RPC interface.</span></div><div class='line' id='LC11'><span class="cm"> *</span></div><div class='line' id='LC12'><span class="cm"> * &#39;&#39;&#39;Note:&#39;&#39;&#39; this is an abstract class (vs. a trait) to maintain java</span></div><div class='line' id='LC13'><span class="cm"> * compatibility, as it has implementation as well as interface.</span></div><div class='line' id='LC14'><span class="cm"> */</span></div><div class='line' id='LC15'><span class="k">abstract</span> <span class="k">class</span> <span class="nc">Service</span><span class="o">[</span><span class="kt">-Req</span>, <span class="kt">+Rep</span><span class="o">]</span> <span class="nc">extends</span> <span class="o">(</span><span class="nc">Req</span> <span class="k">=&gt;</span> <span class="nc">Future</span><span class="o">[</span><span class="kt">Rep</span><span class="o">])</span> <span class="o">{</span></div><div class='line' id='LC16'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">map</span><span class="o">[</span><span class="kt">Req1</span><span class="o">](</span><span class="n">f</span><span class="k">:</span> <span class="kt">Req1</span> <span class="o">=&gt;</span> <span class="nc">Req</span><span class="o">)</span> <span class="k">=</span> <span class="k">new</span> <span class="nc">Service</span><span class="o">[</span><span class="kt">Req1</span>, <span class="kt">Rep</span><span class="o">]</span> <span class="o">{</span></div><div class='line' id='LC17'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">def</span> <span class="n">apply</span><span class="o">(</span><span class="n">req1</span><span class="k">:</span> <span class="kt">Req1</span><span class="o">)</span> <span class="k">=</span> <span class="nc">Service</span><span class="o">.</span><span class="k">this</span><span class="o">.</span><span class="n">apply</span><span class="o">(</span><span class="n">f</span><span class="o">(</span><span class="n">req1</span><span class="o">))</span></div><div class='line' id='LC18'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">release</span><span class="o">()</span> <span class="k">=</span> <span class="nc">Service</span><span class="o">.</span><span class="k">this</span><span class="o">.</span><span class="n">release</span><span class="o">()</span></div><div class='line' id='LC19'>&nbsp;&nbsp;<span class="o">}</span></div><div class='line' id='LC20'><br/></div><div class='line' id='LC21'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC22'><span class="cm">   * This is the method to override/implement to create your own Service.</span></div><div class='line' id='LC23'><span class="cm">   */</span></div><div class='line' id='LC24'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">apply</span><span class="o">(</span><span class="n">request</span><span class="k">:</span> <span class="kt">Req</span><span class="o">)</span><span class="k">:</span> <span class="kt">Future</span><span class="o">[</span><span class="kt">Rep</span><span class="o">]</span></div><div class='line' id='LC25'><br/></div><div class='line' id='LC26'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC27'><span class="cm">   * Relinquishes the use of this service instance. Behavior is</span></div><div class='line' id='LC28'><span class="cm">   * undefined is apply() is called after resources are relinquished.</span></div><div class='line' id='LC29'><span class="cm">   */</span></div><div class='line' id='LC30'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">release</span><span class="o">()</span> <span class="k">=</span> <span class="o">()</span></div><div class='line' id='LC31'><br/></div><div class='line' id='LC32'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC33'><span class="cm">   * Determines whether this service is available (can accept requests</span></div><div class='line' id='LC34'><span class="cm">   * with a reasonable likelihood of success).</span></div><div class='line' id='LC35'><span class="cm">   */</span></div><div class='line' id='LC36'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">isAvailable</span><span class="k">:</span> <span class="kt">Boolean</span> <span class="o">=</span> <span class="kc">true</span></div><div class='line' id='LC37'><span class="o">}</span></div><div class='line' id='LC38'><br/></div><div class='line' id='LC39'><span class="cm">/**</span></div><div class='line' id='LC40'><span class="cm"> * Information about a client, passed to a Service factory for each new</span></div><div class='line' id='LC41'><span class="cm"> * connection.</span></div><div class='line' id='LC42'><span class="cm"> */</span></div><div class='line' id='LC43'><span class="k">trait</span> <span class="nc">ClientConnection</span> <span class="o">{</span></div><div class='line' id='LC44'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC45'><span class="cm">   * Host/port of the client. This is only available after `Service#connected`</span></div><div class='line' id='LC46'><span class="cm">   * has been signalled.</span></div><div class='line' id='LC47'><span class="cm">   */</span></div><div class='line' id='LC48'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">remoteAddress</span><span class="k">:</span> <span class="kt">SocketAddress</span></div><div class='line' id='LC49'><br/></div><div class='line' id='LC50'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC51'><span class="cm">   * Host/port of the local side of a client connection. This is only</span></div><div class='line' id='LC52'><span class="cm">   * available after `Service#connected` has been signalled.</span></div><div class='line' id='LC53'><span class="cm">   */</span></div><div class='line' id='LC54'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">localAddress</span><span class="k">:</span> <span class="kt">SocketAddress</span></div><div class='line' id='LC55'><br/></div><div class='line' id='LC56'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC57'><span class="cm">   * Close the underlying client connection.</span></div><div class='line' id='LC58'><span class="cm">   */</span></div><div class='line' id='LC59'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">close</span><span class="o">()</span></div><div class='line' id='LC60'><span class="o">}</span></div><div class='line' id='LC61'><br/></div><div class='line' id='LC62'><span class="cm">/**</span></div><div class='line' id='LC63'><span class="cm"> * A simple proxy Service that forwards all calls to another Service.</span></div><div class='line' id='LC64'><span class="cm"> * This is is useful if you to wrap-but-modify an existing service.</span></div><div class='line' id='LC65'><span class="cm"> */</span></div><div class='line' id='LC66'><span class="k">abstract</span> <span class="k">class</span> <span class="nc">ServiceProxy</span><span class="o">[</span><span class="kt">-Req</span>, <span class="kt">+Rep</span><span class="o">](</span><span class="k">val</span> <span class="n">self</span><span class="k">:</span> <span class="kt">Service</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">])</span></div><div class='line' id='LC67'>&nbsp;&nbsp;<span class="k">extends</span> <span class="nc">Service</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">]</span> <span class="k">with</span> <span class="nc">Proxy</span></div><div class='line' id='LC68'><span class="o">{</span></div><div class='line' id='LC69'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">apply</span><span class="o">(</span><span class="n">request</span><span class="k">:</span> <span class="kt">Req</span><span class="o">)</span> <span class="k">=</span> <span class="n">self</span><span class="o">(</span><span class="n">request</span><span class="o">)</span></div><div class='line' id='LC70'>&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">release</span><span class="o">()</span> <span class="k">=</span> <span class="n">self</span><span class="o">.</span><span class="n">release</span><span class="o">()</span></div><div class='line' id='LC71'>&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">isAvailable</span> <span class="k">=</span> <span class="n">self</span><span class="o">.</span><span class="n">isAvailable</span></div><div class='line' id='LC72'><span class="o">}</span></div><div class='line' id='LC73'><br/></div><div class='line' id='LC74'><span class="k">abstract</span> <span class="k">class</span> <span class="nc">ServiceFactory</span><span class="o">[</span><span class="kt">-Req</span>, <span class="kt">+Rep</span><span class="o">]</span> <span class="o">{</span></div><div class='line' id='LC75'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC76'><span class="cm">   * Reserve the use of a given service instance. This pins the</span></div><div class='line' id='LC77'><span class="cm">   * underlying channel and the returned service has exclusive use of</span></div><div class='line' id='LC78'><span class="cm">   * its underlying connection. To relinquish the use of the reserved</span></div><div class='line' id='LC79'><span class="cm">   * Service, the user must call Service.release().</span></div><div class='line' id='LC80'><span class="cm">   */</span></div><div class='line' id='LC81'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">make</span><span class="o">()</span><span class="k">:</span> <span class="kt">Future</span><span class="o">[</span><span class="kt">Service</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">]]</span></div><div class='line' id='LC82'><br/></div><div class='line' id='LC83'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC84'><span class="cm">   * Make a service that after dispatching a request on that service,</span></div><div class='line' id='LC85'><span class="cm">   * releases the service.</span></div><div class='line' id='LC86'><span class="cm">   */</span></div><div class='line' id='LC87'>&nbsp;&nbsp;<span class="k">final</span> <span class="k">def</span> <span class="n">service</span><span class="k">:</span> <span class="kt">Service</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">]</span> <span class="k">=</span> <span class="k">new</span> <span class="nc">FactoryToService</span><span class="o">(</span><span class="k">this</span><span class="o">)</span></div><div class='line' id='LC88'><br/></div><div class='line' id='LC89'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC90'><span class="cm">   * Close the factory and its underlying resources.</span></div><div class='line' id='LC91'><span class="cm">   */</span></div><div class='line' id='LC92'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">close</span><span class="o">()</span></div><div class='line' id='LC93'><br/></div><div class='line' id='LC94'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">isAvailable</span><span class="k">:</span> <span class="kt">Boolean</span> <span class="o">=</span> <span class="kc">true</span></div><div class='line' id='LC95'><span class="o">}</span></div><div class='line' id='LC96'><br/></div><div class='line' id='LC97'><span class="cm">/**</span></div><div class='line' id='LC98'><span class="cm"> * A simple proxy ServiceFactory that forwards all calls to another</span></div><div class='line' id='LC99'><span class="cm"> * ServiceFactory.  This is is useful if you to wrap-but-modify an</span></div><div class='line' id='LC100'><span class="cm"> * existing service factory.</span></div><div class='line' id='LC101'><span class="cm"> */</span></div><div class='line' id='LC102'><span class="k">abstract</span> <span class="k">class</span> <span class="nc">ServiceFactoryProxy</span><span class="o">[</span><span class="kt">-Req</span>, <span class="kt">+Rep</span><span class="o">](</span><span class="k">val</span> <span class="n">self</span><span class="k">:</span> <span class="kt">ServiceFactory</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">])</span></div><div class='line' id='LC103'>&nbsp;&nbsp;<span class="k">extends</span> <span class="nc">ServiceFactory</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">]</span> <span class="k">with</span> <span class="nc">Proxy</span></div><div class='line' id='LC104'><span class="o">{</span></div><div class='line' id='LC105'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">make</span><span class="o">()</span> <span class="k">=</span> <span class="n">self</span><span class="o">.</span><span class="n">make</span><span class="o">()</span></div><div class='line' id='LC106'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">close</span><span class="o">()</span> <span class="k">=</span> <span class="n">self</span><span class="o">.</span><span class="n">close</span><span class="o">()</span></div><div class='line' id='LC107'>&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">isAvailable</span> <span class="k">=</span> <span class="n">self</span><span class="o">.</span><span class="n">isAvailable</span></div><div class='line' id='LC108'><span class="o">}</span></div><div class='line' id='LC109'><br/></div><div class='line' id='LC110'><span class="k">class</span> <span class="nc">FactoryToService</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">](</span><span class="n">factory</span><span class="k">:</span> <span class="kt">ServiceFactory</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">])</span></div><div class='line' id='LC111'>&nbsp;&nbsp;<span class="k">extends</span> <span class="nc">Service</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">]</span></div><div class='line' id='LC112'><span class="o">{</span></div><div class='line' id='LC113'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">apply</span><span class="o">(</span><span class="n">request</span><span class="k">:</span> <span class="kt">Req</span><span class="o">)</span> <span class="k">=</span> <span class="o">{</span></div><div class='line' id='LC114'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="n">factory</span><span class="o">.</span><span class="n">make</span><span class="o">()</span> <span class="n">flatMap</span> <span class="o">{</span> <span class="n">service</span> <span class="k">=&gt;</span></div><div class='line' id='LC115'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="n">service</span><span class="o">(</span><span class="n">request</span><span class="o">)</span> <span class="n">ensure</span> <span class="o">{</span> <span class="n">service</span><span class="o">.</span><span class="n">release</span><span class="o">()</span> <span class="o">}</span></div><div class='line' id='LC116'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="o">}</span></div><div class='line' id='LC117'>&nbsp;&nbsp;<span class="o">}</span></div><div class='line' id='LC118'><br/></div><div class='line' id='LC119'>&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">release</span><span class="o">()</span> <span class="k">=</span> <span class="n">factory</span><span class="o">.</span><span class="n">close</span><span class="o">()</span></div><div class='line' id='LC120'>&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">isAvailable</span> <span class="k">=</span> <span class="n">factory</span><span class="o">.</span><span class="n">isAvailable</span></div><div class='line' id='LC121'><span class="o">}</span></div><div class='line' id='LC122'><br/></div><div class='line' id='LC123'><span class="cm">/**</span></div><div class='line' id='LC124'><span class="cm"> *  A Filter acts as a decorator/transformer of a service. It may apply</span></div><div class='line' id='LC125'><span class="cm"> * transformations to the input and output of that service:</span></div><div class='line' id='LC126'><span class="cm"> *</span></div><div class='line' id='LC127'><span class="cm"> *           (*  MyService  *)</span></div><div class='line' id='LC128'><span class="cm"> * [ReqIn -&gt; (ReqOut -&gt; RepIn) -&gt; RepOut]</span></div><div class='line' id='LC129'><span class="cm"> *</span></div><div class='line' id='LC130'><span class="cm"> * For example, you may have a POJO service that takes Strings and</span></div><div class='line' id='LC131'><span class="cm"> * parses them as Ints.  If you want to expose this as a Network</span></div><div class='line' id='LC132'><span class="cm"> * Service via Thrift, it is nice to isolate the protocol handling</span></div><div class='line' id='LC133'><span class="cm"> * from the business rules. Hence you might have a Filter that</span></div><div class='line' id='LC134'><span class="cm"> * converts back and forth between Thrift structs. Again, your service</span></div><div class='line' id='LC135'><span class="cm"> * deals with POJOs:</span></div><div class='line' id='LC136'><span class="cm"> *</span></div><div class='line' id='LC137'><span class="cm"> * [ThriftIn -&gt; (String  -&gt;  Int) -&gt; ThriftOut]</span></div><div class='line' id='LC138'><span class="cm"> *</span></div><div class='line' id='LC139'><span class="cm"> */</span></div><div class='line' id='LC140'><span class="k">abstract</span> <span class="k">class</span> <span class="nc">Filter</span><span class="o">[</span><span class="kt">-ReqIn</span>, <span class="kt">+RepOut</span>, <span class="kt">+ReqOut</span>, <span class="kt">-RepIn</span><span class="o">]</span></div><div class='line' id='LC141'>&nbsp;&nbsp;<span class="nc">extends</span> <span class="o">((</span><span class="nc">ReqIn</span><span class="o">,</span> <span class="nc">Service</span><span class="o">[</span><span class="kt">ReqOut</span>, <span class="kt">RepIn</span><span class="o">])</span> <span class="k">=&gt;</span> <span class="nc">Future</span><span class="o">[</span><span class="kt">RepOut</span><span class="o">])</span></div><div class='line' id='LC142'><span class="o">{</span></div><div class='line' id='LC143'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC144'><span class="cm">   * This is the method to override/implement to create your own Filter.</span></div><div class='line' id='LC145'><span class="cm">   *</span></div><div class='line' id='LC146'><span class="cm">   * @param  request  the input request type</span></div><div class='line' id='LC147'><span class="cm">   * @param  service  a service that takes the output request type and the input response type</span></div><div class='line' id='LC148'><span class="cm">   *</span></div><div class='line' id='LC149'><span class="cm">   */</span></div><div class='line' id='LC150'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">apply</span><span class="o">(</span><span class="n">request</span><span class="k">:</span> <span class="kt">ReqIn</span><span class="o">,</span> <span class="n">service</span><span class="k">:</span> <span class="kt">Service</span><span class="o">[</span><span class="kt">ReqOut</span>, <span class="kt">RepIn</span><span class="o">])</span><span class="k">:</span> <span class="kt">Future</span><span class="o">[</span><span class="kt">RepOut</span><span class="o">]</span></div><div class='line' id='LC151'><br/></div><div class='line' id='LC152'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC153'><span class="cm">   * Chains a series of filters together:</span></div><div class='line' id='LC154'><span class="cm">   *</span></div><div class='line' id='LC155'><span class="cm">   *    myModularService = handleExceptions.andThen(thrift2Pojo.andThen(parseString))</span></div><div class='line' id='LC156'><span class="cm">   *</span></div><div class='line' id='LC157'><span class="cm">   * @param  next  another filter to follow after this one</span></div><div class='line' id='LC158'><span class="cm">   *</span></div><div class='line' id='LC159'><span class="cm">   */</span></div><div class='line' id='LC160'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">andThen</span><span class="o">[</span><span class="kt">Req2</span>, <span class="kt">Rep2</span><span class="o">](</span><span class="n">next</span><span class="k">:</span> <span class="kt">Filter</span><span class="o">[</span><span class="kt">ReqOut</span>, <span class="kt">RepIn</span>, <span class="kt">Req2</span>, <span class="kt">Rep2</span><span class="o">])</span> <span class="k">=</span></div><div class='line' id='LC161'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">new</span> <span class="nc">Filter</span><span class="o">[</span><span class="kt">ReqIn</span>, <span class="kt">RepOut</span>, <span class="kt">Req2</span>, <span class="kt">Rep2</span><span class="o">]</span> <span class="o">{</span></div><div class='line' id='LC162'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">def</span> <span class="n">apply</span><span class="o">(</span><span class="n">request</span><span class="k">:</span> <span class="kt">ReqIn</span><span class="o">,</span> <span class="n">service</span><span class="k">:</span> <span class="kt">Service</span><span class="o">[</span><span class="kt">Req2</span>, <span class="kt">Rep2</span><span class="o">])</span> <span class="k">=</span> <span class="o">{</span></div><div class='line' id='LC163'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="nc">Filter</span><span class="o">.</span><span class="k">this</span><span class="o">.</span><span class="n">apply</span><span class="o">(</span><span class="n">request</span><span class="o">,</span> <span class="k">new</span> <span class="nc">Service</span><span class="o">[</span><span class="kt">ReqOut</span>, <span class="kt">RepIn</span><span class="o">]</span> <span class="o">{</span></div><div class='line' id='LC164'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">def</span> <span class="n">apply</span><span class="o">(</span><span class="n">request</span><span class="k">:</span> <span class="kt">ReqOut</span><span class="o">)</span><span class="k">:</span> <span class="kt">Future</span><span class="o">[</span><span class="kt">RepIn</span><span class="o">]</span> <span class="k">=</span> <span class="n">next</span><span class="o">(</span><span class="n">request</span><span class="o">,</span> <span class="n">service</span><span class="o">)</span></div><div class='line' id='LC165'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">release</span><span class="o">()</span> <span class="k">=</span> <span class="n">service</span><span class="o">.</span><span class="n">release</span><span class="o">()</span></div><div class='line' id='LC166'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">isAvailable</span> <span class="k">=</span> <span class="n">service</span><span class="o">.</span><span class="n">isAvailable</span></div><div class='line' id='LC167'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="o">})</span></div><div class='line' id='LC168'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="o">}</span></div><div class='line' id='LC169'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="o">}</span></div><div class='line' id='LC170'><br/></div><div class='line' id='LC171'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC172'><span class="cm">   * Terminates a filter chain in a service. For example,</span></div><div class='line' id='LC173'><span class="cm">   *</span></div><div class='line' id='LC174'><span class="cm">   *     myFilter.andThen(myService)</span></div><div class='line' id='LC175'><span class="cm">   *</span></div><div class='line' id='LC176'><span class="cm">   * @param  service  a service that takes the output request type and the input response type.</span></div><div class='line' id='LC177'><span class="cm">   *</span></div><div class='line' id='LC178'><span class="cm">   */</span></div><div class='line' id='LC179'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">andThen</span><span class="o">(</span><span class="n">service</span><span class="k">:</span> <span class="kt">Service</span><span class="o">[</span><span class="kt">ReqOut</span>, <span class="kt">RepIn</span><span class="o">])</span> <span class="k">=</span> <span class="k">new</span> <span class="nc">Service</span><span class="o">[</span><span class="kt">ReqIn</span>, <span class="kt">RepOut</span><span class="o">]</span> <span class="o">{</span></div><div class='line' id='LC180'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">private</span><span class="o">[</span><span class="kt">this</span><span class="o">]</span> <span class="k">val</span> <span class="n">refcounted</span> <span class="k">=</span> <span class="k">new</span> <span class="nc">RefcountedService</span><span class="o">(</span><span class="n">service</span><span class="o">)</span></div><div class='line' id='LC181'><br/></div><div class='line' id='LC182'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">def</span> <span class="n">apply</span><span class="o">(</span><span class="n">request</span><span class="k">:</span> <span class="kt">ReqIn</span><span class="o">)</span> <span class="k">=</span> <span class="nc">Filter</span><span class="o">.</span><span class="k">this</span><span class="o">.</span><span class="n">apply</span><span class="o">(</span><span class="n">request</span><span class="o">,</span> <span class="n">refcounted</span><span class="o">)</span></div><div class='line' id='LC183'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">release</span><span class="o">()</span> <span class="k">=</span> <span class="n">refcounted</span><span class="o">.</span><span class="n">release</span><span class="o">()</span></div><div class='line' id='LC184'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">isAvailable</span> <span class="k">=</span> <span class="n">refcounted</span><span class="o">.</span><span class="n">isAvailable</span></div><div class='line' id='LC185'>&nbsp;&nbsp;<span class="o">}</span></div><div class='line' id='LC186'><br/></div><div class='line' id='LC187'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">andThen</span><span class="o">(</span><span class="n">factory</span><span class="k">:</span> <span class="kt">ServiceFactory</span><span class="o">[</span><span class="kt">ReqOut</span>, <span class="kt">RepIn</span><span class="o">])</span><span class="k">:</span> <span class="kt">ServiceFactory</span><span class="o">[</span><span class="kt">ReqIn</span>, <span class="kt">RepOut</span><span class="o">]</span> <span class="k">=</span></div><div class='line' id='LC188'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">new</span> <span class="nc">ServiceFactory</span><span class="o">[</span><span class="kt">ReqIn</span>, <span class="kt">RepOut</span><span class="o">]</span> <span class="o">{</span></div><div class='line' id='LC189'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">def</span> <span class="n">make</span><span class="o">()</span> <span class="k">=</span> <span class="n">factory</span><span class="o">.</span><span class="n">make</span><span class="o">()</span> <span class="n">map</span> <span class="o">{</span> <span class="nc">Filter</span><span class="o">.</span><span class="k">this</span> <span class="n">andThen</span> <span class="k">_</span> <span class="o">}</span></div><div class='line' id='LC190'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">close</span><span class="o">()</span> <span class="k">=</span> <span class="n">factory</span><span class="o">.</span><span class="n">close</span><span class="o">()</span></div><div class='line' id='LC191'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">isAvailable</span> <span class="k">=</span> <span class="n">factory</span><span class="o">.</span><span class="n">isAvailable</span></div><div class='line' id='LC192'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">override</span> <span class="k">def</span> <span class="n">toString</span> <span class="k">=</span> <span class="n">factory</span><span class="o">.</span><span class="n">toString</span></div><div class='line' id='LC193'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="o">}</span></div><div class='line' id='LC194'><br/></div><div class='line' id='LC195'>&nbsp;&nbsp;<span class="cm">/**</span></div><div class='line' id='LC196'><span class="cm">   * Conditionally propagates requests down the filter chain. This may</span></div><div class='line' id='LC197'><span class="cm">   * useful if you are statically wiring together filter chains based</span></div><div class='line' id='LC198'><span class="cm">   * on a configuration file, for instance.</span></div><div class='line' id='LC199'><span class="cm">   *</span></div><div class='line' id='LC200'><span class="cm">   * @param  condAndFilter  a tuple of boolean and filter.</span></div><div class='line' id='LC201'><span class="cm">   *</span></div><div class='line' id='LC202'><span class="cm">   */</span></div><div class='line' id='LC203'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">andThenIf</span><span class="o">[</span><span class="kt">Req2</span> <span class="k">&gt;:</span> <span class="kt">ReqOut</span>, <span class="kt">Rep2</span> <span class="k">&lt;:</span> <span class="kt">RepIn</span><span class="o">](</span></div><div class='line' id='LC204'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="n">condAndFilter</span><span class="k">:</span> <span class="o">(</span><span class="kt">Boolean</span><span class="o">,</span> <span class="kt">Filter</span><span class="o">[</span><span class="kt">ReqOut</span>, <span class="kt">RepIn</span>, <span class="kt">Req2</span>, <span class="kt">Rep2</span><span class="o">]))</span> <span class="k">=</span></div><div class='line' id='LC205'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="n">condAndFilter</span> <span class="k">match</span> <span class="o">{</span></div><div class='line' id='LC206'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">case</span> <span class="o">(</span><span class="kc">true</span><span class="o">,</span> <span class="n">filter</span><span class="o">)</span> <span class="k">=&gt;</span> <span class="n">andThen</span><span class="o">(</span><span class="n">filter</span><span class="o">)</span></div><div class='line' id='LC207'>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">case</span> <span class="o">(</span><span class="kc">false</span><span class="o">,</span> <span class="n">_</span><span class="o">)</span>     <span class="k">=&gt;</span> <span class="k">this</span></div><div class='line' id='LC208'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="o">}</span></div><div class='line' id='LC209'><span class="o">}</span></div><div class='line' id='LC210'><br/></div><div class='line' id='LC211'><span class="k">abstract</span> <span class="k">class</span> <span class="nc">SimpleFilter</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">]</span> <span class="nc">extends</span> <span class="nc">Filter</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span>, <span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">]</span></div><div class='line' id='LC212'><br/></div><div class='line' id='LC213'><span class="k">object</span> <span class="nc">Filter</span> <span class="o">{</span></div><div class='line' id='LC214'>&nbsp;&nbsp;<span class="k">def</span> <span class="n">identity</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">]</span> <span class="k">=</span> <span class="k">new</span> <span class="nc">SimpleFilter</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">]</span> <span class="o">{</span></div><div class='line' id='LC215'>&nbsp;&nbsp;&nbsp;&nbsp;<span class="k">def</span> <span class="n">apply</span><span class="o">(</span><span class="n">request</span><span class="k">:</span> <span class="kt">Req</span><span class="o">,</span> <span class="n">service</span><span class="k">:</span> <span class="kt">Service</span><span class="o">[</span><span class="kt">Req</span>, <span class="kt">Rep</span><span class="o">])</span> <span class="k">=</span> <span class="n">service</span><span class="o">(</span><span class="n">request</span><span class="o">)</span></div><div class='line' id='LC216'>&nbsp;&nbsp;<span class="o">}</span></div><div class='line' id='LC217'><span class="o">}</span></div><div class='line' id='LC218'><br/></div></pre></div>
          </td>
        </tr>
      </table>
  </div>

          </div>
        </div>
      </div>
    </div>

  </div>

<div class="frame frame-loading" style="display:none;" data-tree-list-url="/mccue/finagle/tree-list/338669002c4a707936b65400a24e851482cc5977" data-blob-url-prefix="/mccue/finagle/blob/338669002c4a707936b65400a24e851482cc5977">
  <img src="https://a248.e.akamai.net/assets.github.com/images/modules/ajax/big_spinner_336699.gif" height="32" width="32">
</div>

    </div>

    </div>

    <!-- footer -->
    <div id="footer" >
      
  <div class="upper_footer">
     <div class="site" class="clearfix">

       <!--[if IE]><h4 id="blacktocat_ie">GitHub Links</h4><![endif]-->
       <![if !IE]><h4 id="blacktocat">GitHub Links</h4><![endif]>

       <ul class="footer_nav">
         <h4>GitHub</h4>
         <li><a href="https://github.com/about">About</a></li>
         <li><a href="https://github.com/blog">Blog</a></li>
         <li><a href="https://github.com/features">Features</a></li>
         <li><a href="https://github.com/contact">Contact &amp; Support</a></li>
         <li><a href="https://github.com/training">Training</a></li>
         <li><a href="http://status.github.com/">Site Status</a></li>
       </ul>

       <ul class="footer_nav">
         <h4>Tools</h4>
         <li><a href="http://mac.github.com/">GitHub for Mac</a></li>
         <li><a href="http://mobile.github.com/">Issues for iPhone</a></li>
         <li><a href="https://gist.github.com">Gist: Code Snippets</a></li>
         <li><a href="http://fi.github.com/">Enterprise Install</a></li>
         <li><a href="http://jobs.github.com/">Job Board</a></li>
       </ul>

       <ul class="footer_nav">
         <h4>Extras</h4>
         <li><a href="http://shop.github.com/">GitHub Shop</a></li>
         <li><a href="http://octodex.github.com/">The Octodex</a></li>
       </ul>

       <ul class="footer_nav">
         <h4>Documentation</h4>
         <li><a href="http://help.github.com/">GitHub Help</a></li>
         <li><a href="http://developer.github.com/">Developer API</a></li>
         <li><a href="http://github.github.com/github-flavored-markdown/">GitHub Flavored Markdown</a></li>
         <li><a href="http://pages.github.com/">GitHub Pages</a></li>
       </ul>

     </div><!-- /.site -->
  </div><!-- /.upper_footer -->

<div class="lower_footer">
  <div class="site" class="clearfix">
    <!--[if IE]><div id="legal_ie"><![endif]-->
    <![if !IE]><div id="legal"><![endif]>
      <ul>
        <li><a href="https://github.com/site/terms">Terms of Service</a></li>
        <li><a href="https://github.com/site/privacy">Privacy</a></li>
        <li><a href="https://github.com/security">Security</a></li>
      </ul>

      <p>&copy; 2011 <span id="_rrt" title="0.17997s from fe8.rs.github.com">GitHub</span> Inc. All rights reserved.</p>
    </div><!-- /#legal or /#legal_ie-->

      <div class="sponsor">
        <a href="http://www.rackspace.com" class="logo">
          <img alt="Dedicated Server" height="36" src="https://a248.e.akamai.net/assets.github.com/images/modules/footer/rackspace_logo.png?v2" width="38" />
        </a>
        Powered by the <a href="http://www.rackspace.com ">Dedicated
        Servers</a> and<br/> <a href="http://www.rackspacecloud.com">Cloud
        Computing</a> of Rackspace Hosting<span>&reg;</span>
      </div>
  </div><!-- /.site -->
</div><!-- /.lower_footer -->

    </div><!-- /#footer -->

    

<div id="keyboard_shortcuts_pane" class="instapaper_ignore readability-extra" style="display:none">
  <h2>Keyboard Shortcuts <small><a href="#" class="js-see-all-keyboard-shortcuts">(see all)</a></small></h2>

  <div class="columns threecols">
    <div class="column first">
      <h3>Site wide shortcuts</h3>
      <dl class="keyboard-mappings">
        <dt>s</dt>
        <dd>Focus site search</dd>
      </dl>
      <dl class="keyboard-mappings">
        <dt>?</dt>
        <dd>Bring up this help dialog</dd>
      </dl>
    </div><!-- /.column.first -->

    <div class="column middle" style='display:none'>
      <h3>Commit list</h3>
      <dl class="keyboard-mappings">
        <dt>j</dt>
        <dd>Move selected down</dd>
      </dl>
      <dl class="keyboard-mappings">
        <dt>k</dt>
        <dd>Move selected up</dd>
      </dl>
      <dl class="keyboard-mappings">
        <dt>c <em>or</em> o <em>or</em> enter</dt>
        <dd>Open commit</dd>
      </dl>
      <dl class="keyboard-mappings">
        <dt>y</dt>
        <dd>Expand URL to its canonical form</dd>
      </dl>
    </div><!-- /.column.first -->

    <div class="column last" style='display:none'>
      <h3>Pull request list</h3>
      <dl class="keyboard-mappings">
        <dt>j</dt>
        <dd>Move selected down</dd>
      </dl>
      <dl class="keyboard-mappings">
        <dt>k</dt>
        <dd>Move selected up</dd>
      </dl>
      <dl class="keyboard-mappings">
        <dt>o <em>or</em> enter</dt>
        <dd>Open issue</dd>
      </dl>
    </div><!-- /.columns.last -->

  </div><!-- /.columns.equacols -->

  <div style='display:none'>
    <div class="rule"></div>

    <h3>Issues</h3>

    <div class="columns threecols">
      <div class="column first">
        <dl class="keyboard-mappings">
          <dt>j</dt>
          <dd>Move selected down</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>k</dt>
          <dd>Move selected up</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>x</dt>
          <dd>Toggle select target</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>o <em>or</em> enter</dt>
          <dd>Open issue</dd>
        </dl>
      </div><!-- /.column.first -->
      <div class="column middle">
        <dl class="keyboard-mappings">
          <dt>I</dt>
          <dd>Mark selected as read</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>U</dt>
          <dd>Mark selected as unread</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>e</dt>
          <dd>Close selected</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>y</dt>
          <dd>Remove selected from view</dd>
        </dl>
      </div><!-- /.column.middle -->
      <div class="column last">
        <dl class="keyboard-mappings">
          <dt>c</dt>
          <dd>Create issue</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>l</dt>
          <dd>Create label</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>i</dt>
          <dd>Back to inbox</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>u</dt>
          <dd>Back to issues</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>/</dt>
          <dd>Focus issues search</dd>
        </dl>
      </div>
    </div>
  </div>

  <div style='display:none'>
    <div class="rule"></div>

    <h3>Issues Dashboard</h3>

    <div class="columns threecols">
      <div class="column first">
        <dl class="keyboard-mappings">
          <dt>j</dt>
          <dd>Move selected down</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>k</dt>
          <dd>Move selected up</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>o <em>or</em> enter</dt>
          <dd>Open issue</dd>
        </dl>
      </div><!-- /.column.first -->
    </div>
  </div>

  <div style='display:none'>
    <div class="rule"></div>

    <h3>Network Graph</h3>
    <div class="columns equacols">
      <div class="column first">
        <dl class="keyboard-mappings">
          <dt><span class="badmono">←</span> <em>or</em> h</dt>
          <dd>Scroll left</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt><span class="badmono">→</span> <em>or</em> l</dt>
          <dd>Scroll right</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt><span class="badmono">↑</span> <em>or</em> k</dt>
          <dd>Scroll up</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt><span class="badmono">↓</span> <em>or</em> j</dt>
          <dd>Scroll down</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>t</dt>
          <dd>Toggle visibility of head labels</dd>
        </dl>
      </div><!-- /.column.first -->
      <div class="column last">
        <dl class="keyboard-mappings">
          <dt>shift <span class="badmono">←</span> <em>or</em> shift h</dt>
          <dd>Scroll all the way left</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>shift <span class="badmono">→</span> <em>or</em> shift l</dt>
          <dd>Scroll all the way right</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>shift <span class="badmono">↑</span> <em>or</em> shift k</dt>
          <dd>Scroll all the way up</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>shift <span class="badmono">↓</span> <em>or</em> shift j</dt>
          <dd>Scroll all the way down</dd>
        </dl>
      </div><!-- /.column.last -->
    </div>
  </div>

  <div >
    <div class="rule"></div>
    <div class="columns threecols">
      <div class="column first" >
        <h3>Source Code Browsing</h3>
        <dl class="keyboard-mappings">
          <dt>t</dt>
          <dd>Activates the file finder</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>l</dt>
          <dd>Jump to line</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>w</dt>
          <dd>Switch branch/tag</dd>
        </dl>
        <dl class="keyboard-mappings">
          <dt>y</dt>
          <dd>Expand URL to its canonical form</dd>
        </dl>
      </div>
    </div>
  </div>
</div>

    <div id="markdown-help" class="instapaper_ignore readability-extra">
  <h2>Markdown Cheat Sheet</h2>

  <div class="cheatsheet-content">

  <div class="mod">
    <div class="col">
      <h3>Format Text</h3>
      <p>Headers</p>
      <pre>
# This is an &lt;h1&gt; tag
## This is an &lt;h2&gt; tag
###### This is an &lt;h6&gt; tag</pre>
     <p>Text styles</p>
     <pre>
*This text will be italic*
_This will also be italic_
**This text will be bold**
__This will also be bold__

*You **can** combine them*
</pre>
    </div>
    <div class="col">
      <h3>Lists</h3>
      <p>Unordered</p>
      <pre>
* Item 1
* Item 2
  * Item 2a
  * Item 2b</pre>
     <p>Ordered</p>
     <pre>
1. Item 1
2. Item 2
3. Item 3
   * Item 3a
   * Item 3b</pre>
    </div>
    <div class="col">
      <h3>Miscellaneous</h3>
      <p>Images</p>
      <pre>
![GitHub Logo](/images/logo.png)
Format: ![Alt Text](url)
</pre>
     <p>Links</p>
     <pre>
http://github.com - automatic!
[GitHub](http://github.com)</pre>
<p>Blockquotes</p>
     <pre>
As Kanye West said:
> We're living the future so
> the present is our past.
</pre>
    </div>
  </div>
  <div class="rule"></div>

  <h3>Code Examples in Markdown</h3>
  <div class="col">
      <p>Syntax highlighting with <a href="http://github.github.com/github-flavored-markdown/" title="GitHub Flavored Markdown" target="_blank">GFM</a></p>
      <pre>
```javascript
function fancyAlert(arg) {
  if(arg) {
    $.facebox({div:'#foo'})
  }
}
```</pre>
    </div>
    <div class="col">
      <p>Or, indent your code 4 spaces</p>
      <pre>
Here is a Python code example
without syntax highlighting:

    def foo:
      if not bar:
        return true</pre>
    </div>
    <div class="col">
      <p>Inline code for comments</p>
      <pre>
I think you should use an
`&lt;addr&gt;` element here instead.</pre>
    </div>
  </div>

  </div>
</div>

    <div class="context-overlay"></div>

    
    
    
  </body>
</html>

