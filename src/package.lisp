(defpackage :cl-rabbit-async
  (:use :cl)
  (:documentation "Experimental async version of cl-rabbit")
  (:export #:async-channel
           #:async-connection
           #:make-async-connection
           #:close-async-connection
           #:open-channel
           #:close-channel
           #:async-exchange-declare
           #:async-exchange-delete
           #:async-exchange-unbind
           #:async-exchange-bind
           #:async-queue-declare
           #:async-queue-bind
           #:async-queue-unbind
           #:async-queue-purge
           #:async-queue-delete
           #:async-basic-consume
           #:async-basic-publish
           #:make-multi-connection
           #:multi-connection-open-channel
           #:async-basic-ack
           #:async-basic-nack
           #:close-multi-connection
           #:async-tx-select
           #:async-tx-commit
           #:async-tx-rollback))
