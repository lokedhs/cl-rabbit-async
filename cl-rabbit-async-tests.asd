(asdf:defsystem :cl-rabbit-async-tests
  :name "cl-rabbit-async-tests"
  :author "Elias Martenson <lokedhs@gmail.com>"
  :license "MIT"
  :description "Fiveam testcases for cl-rabbit-async"
  :depends-on (:cl-rabbit-async
               :fiveam)
  :components ((:module tests
                        :serial t
                        :components ((:file "package")
                                     (:file "tests")))))
