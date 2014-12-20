{
  "targets": [
    {
      "target_name": "cproton",
      "type": "loadable_module",
      "sources": [ "src/cproton.cc", "src/sending.cc", "src/messenger.cc", "src/protondata.cc" ],
  
      'conditions': [
        ['OS=="linux"', {
          "link_settings" : {
            "libraries": [ 
              '-L/usr/local/qpid-proton/lib',
              '-lqpid-proton' 
            ],
          },
          "include_dirs": [ 
            '/usr/include/nodejs/src',
            '/usr/include/nodejs/deps/uv/include/',
            '/usr/include/nodejs/deps/v8/include/',
            '/usr/local/qpid-proton/include'
          ],
          "cflags": [
            "-fPIC"
          ]
        }],
        ['OS=="mac"', {
          'defines!': [
            'PLATFORM="mac"',
          ],
          'defines': [
            # we need to use node's preferred "darwin" rather than gyp's preferred "mac"
            'PLATFORM="darwin"',
          ],
          "link_settings" : {
            "libraries": [ 
              '-lqpid-proton',
              '-L/usr/local/qpid-proton/lib64'
            ],
          },
          "include_dirs": [ 
            '/usr/local/qpid-proton/include', 
            '/usr/local/node/include/node',
          ],
          "cflags": [
            "-fPIC"
          ]
        }]
      ]
    }
  ]
}
