(in-package :cl-rabbit-async)

(declaim (optimize (safety 3) (debug 3) (speed 0)))

(defclass mconnection-wrapper ()
  ((connection  :type async-connection
                :initarg :connection
                :reader mconnection-wrapper/connection)
   (in-progress :type integer
                :initform 0
                :accessor mconnection-wrapper/in-progress)))

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
                 :documentation "List of all active connections.")
   (lock         :type t
                 :initform (bordeaux-threads:make-lock)
                 :reader multi-connection/lock)
   (closed       :type t
                 :initform nil
                 :accessor multi-connection/closed))
  (:documentation "Tracks multiple connections to RabbitMQ"))

(defun make-multi-connection (hostname &key (port 5672) (user "guest") (password "guest") (vhost "/") (max-channels 10))
  (make-instance 'multi-connection
                 :hostname hostname
                 :port port
                 :user user
                 :password password
                 :vhost vhost
                 :max-channels max-channels))

(defun multi-connection-remove-if-empty (mconn wrapper)
  (when (zerop (mconnection-wrapper/in-progress wrapper))
    (if (zerop (num-opened-channels (mconnection-wrapper/connection wrapper)))
        (progn
          (setf (multi-connection/connections mconn)
                (remove wrapper (multi-connection/connections mconn)))
          t)
        nil)))

(defun multi-connection-open-channel (mconn &key message-callback close-callback)
  (let ((m (multi-connection/max-channels mconn)))
    (let ((conn (bordeaux-threads:with-lock-held ((multi-connection/lock mconn))
                  (when (multi-connection/closed mconn)
                    (error "Connection is closed"))
                  (loop
                     for conn in (multi-connection/connections mconn)
                     when (< (num-opened-channels (mconnection-wrapper/connection conn)) m)
                     return (progn
                              (incf (mconnection-wrapper/in-progress conn))
                              conn)
                     finally (let* ((conn (make-async-connection (multi-connection/hostname mconn)
                                                                 :port (multi-connection/port mconn)
                                                                 :user (multi-connection/user mconn)
                                                                 :password (multi-connection/password mconn)
                                                                 :vhost (multi-connection/vhost mconn)))
                                    (wrapper (make-instance 'mconnection-wrapper :connection conn)))
                               (push wrapper (multi-connection/connections mconn))
                               (incf (mconnection-wrapper/in-progress wrapper))
                               (return wrapper))))))
      (unwind-protect
           (open-channel (mconnection-wrapper/connection conn)
                         :message-callback message-callback
                         :close-callback (lambda (channel)
                                           (when close-callback
                                             (funcall close-callback channel))
                                           (when (bordeaux-threads:with-lock-held ((multi-connection/lock mconn))
                                                   (multi-connection-remove-if-empty mconn conn))
                                             (close-async-connection (mconnection-wrapper/connection conn)))))
        (when (bordeaux-threads:with-lock-held ((multi-connection/lock mconn))
                (decf (mconnection-wrapper/in-progress conn))
                (multi-connection-remove-if-empty mconn conn))
          (close-async-connection (mconnection-wrapper/connection conn)))))))

(defun close-multi-connection (mconn)
  (let ((result (bordeaux-threads:with-lock-held ((multi-connection/lock mconn))
                  (when (multi-connection/closed mconn)
                    (warn "Connection already closed")
                    nil)
                  (setf (multi-connection/closed mconn) t)
                  t)))
    (when result
      (dolist (conn (multi-connection/connections mconn))
        (close-async-connection (mconnection-wrapper/connection conn))))))
