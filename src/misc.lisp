(in-package :cl-rabbit-async)

(defclass transfer-handler ()
  ((value-p :type t
            :initform nil
            :accessor transfer-handler/value-p)
   (value   :type t
            :initform nil
            :accessor transfer-handler/value)
   (lock    :type t
            :initform (bordeaux-threads:make-lock "Transfer handler lock")
            :reader transfer-handler/lock)
   (condvar :type t
            :initform (bordeaux-threads:make-condition-variable :name "Transfer handler condvar")
            :reader transfer-handler/condvar)))

(defun make-transfer-handler ()
  (make-instance 'transfer-handler))

(defun wait-for-value (h)
  (with-accessors ((value-p transfer-handler/value-p)
                   (value transfer-handler/value))
      h
    (bordeaux-threads:with-lock-held ((transfer-handler/lock h))
      (loop
         while (not value-p)
         do (bordeaux-threads:condition-wait (transfer-handler/condvar h)
                                             (transfer-handler/lock h)))
      (let ((result-value value))
        (setf value-p nil)
        (setf value nil)
        result-value))))

(defun set-value (h new-value)
  (with-accessors ((value-p transfer-handler/value-p)
                   (value transfer-handler/value))
      h
    (bordeaux-threads:with-lock-held ((transfer-handler/lock h))
      (when value-p
        (error "Value is already set"))
      (setf value new-value)
      (setf value-p t)
      (bordeaux-threads:condition-notify (transfer-handler/condvar h))
      new-value)))

(defun condition-broadcast (condvar)
  #+sbcl
  (sb-thread:condition-broadcast condvar)
  #+abcl
  (threads:object-notify-all condvar)
  #-(or sbcl abcl)
  (error "Condition broadcast not implemented"))
