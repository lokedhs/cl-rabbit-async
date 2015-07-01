(asdf:defsystem :cl-rabbit-async
  :name "cl-rabbit-async"
  :author "Elias Martenson <lokedhs@gmail.com>"
  :license "MIT"
  :description "Experimental async version of cl-rabbit"
  :depends-on (:cl-rabbit
               :log4cl
               :trivial-garbage)
  :components ((:module src
                        :serial t
                        :components ((:file "package")
                                     (:file "misc")
                                     (:file "amqp-misc")
                                     (:file "cl-rabbit-async")))))
