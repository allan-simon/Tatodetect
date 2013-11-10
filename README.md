Tatodetect
==========

A webservice to detect the language of a given text

for the moment it's made to provide this simple but powerful API call

```
YOUR_HOST/api/detects/simple?query=YOUR_TEXT
```

and will reply to you the ISO-639-3 alpha 3 code of most probable language

for example 

```
example.com/api/detects/simple?query=あの人って奥さ
```

will have the reply

```
jpn
```


Dependency
==========

  * cmake (only for compilation)
  * a c++ compiler which support c++11 (should be possible to hack the code to make it works with any compiler)
  * sqlite3 
  * cppcms
  * cppcms_skel (an other project of mine see https://github.com/allan-simon/cppcms-skeleton)
  * python3 (only for the generation script)

Compiling it
============

This project is based on cmake 

```
   mkdir build
   cd build 
   cmake ..
   make

```


How to use it
=============

you need to first generate a database from Tatoeba's dump, for this you can use the python script in tools


```

   cd tools 
   python3 generate.py
   
```

after go back in your `build` directory and launch 


```
./Tatodetect -c ../config.js
```


after that you should be able to use it 




