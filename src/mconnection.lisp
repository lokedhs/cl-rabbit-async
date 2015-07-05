(in-package :cl-rabbit-async)

(declaim (optimize (safety 3) (debug 3) (speed 0)))

(defclass multi-connection ()
  ((hostname     :type string
                 :initarg :hostname
                 :reader multi-connection/hostname)
   (port         :type integer
                 :initarg :port
                 :reader multi-connection/port)
   (user         :type string
                 :initarg :user
                 :reader multi-connection/user)
   (password     :type string
                 :initarg :password
                 :reader multi-connection/password)
   (vhost        :type string
                 :initarg :vhost
                 :reader multi-connection/vhost)
   (max-channels :type integer
                 :initform 10
                 :initarg :max-channels
                 :reader multi-connection/max-channels
                 :documentation "The maximum number of active channels per connection")
   (connections  :type list
                 :initform nil
                 :accessor multi-connection/connections
                 :documentation "List of all active connections")
   (lock         :type t
                 :initform (bordeaux-threads:make-lock)
                 :reader multi-connection/lock))
  (:documentation "Tracks multiple connections to RabbitMQ"))

(defun make-multi-connection (hostname &key (port 5672) (user "guest") (password "guest") (vhost "/") (max-channels 10))
  (make-instance 'multi-connection
                 :hostname hostname
                 :port port
                 :user user
                 :password password
                 :vhost vhost
                 :max-channels max-channels))

(defun multi-connection-open-channel (mconn &key message-callback close-callback)
  (let ((m (multi-connection/max-channels mconn)))
    (let ((conn (bordeaux-threads:with-lock-held ((multi-connection/lock mconn))
                  (loop
                     for conn in (multi-connection/connections mconn)
                     when (< (num-opened-channels conn) m)
                     return conn
                     finally (let ((conn (make-async-connection (multi-connection/hostname mconn)
                                                                :port (multi-connection/port mconn)
                                                                :user (multi-connection/user mconn)
                                                                :password (multi-connection/password mconn)
                                                                :vhost (multi-connection/vhost mconn))))
                               (push conn (multi-connection/connections mconn))
                               (return conn))))))
      (open-channel conn :message-callback message-callback :close-callback close-callback))))
