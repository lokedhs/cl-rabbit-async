(in-package :cl-rabbit-async)

(defclass async-channel ()
  ((connection        :type async-connection
                      :initarg :connection
                      :reader async-channel/connection)
   (channel           :type integer
                      :initarg :channel
                      :reader async-channel/channel)
   (message-callbacks :type list
                      :initform nil
                      :accessor async-channel/message-callbacks)
   (close-callbacks   :type list
                      :initform nil
                      :accessor async-channel/close-callbacks)))

(defclass async-connection ()
  ((connection    :type cl-rabbit::connection
                  :initarg :connection
                  :reader async-connection/connection)
   (channels      :type vector
                  :initform (make-array (list 10)
                                        :element-type '(or null async-channel)
                                        :initial-element nil
                                        :adjustable t
                                        :fill-pointer nil)
                  :reader async-connection/channels)
   (cmd-fd        :type fixnum
                  :accessor async-connection/cmd-fd)
   (requests      :type list
                  :initform nil
                  :accessor async-connection/requests)
   (request-index :type integer
                  :initform 0
                  :accessor async-connection/request-index)
   (lock          :type t
                  :initform (bordeaux-threads:make-lock "Sync connection lock")
                  :reader async-connection/lock)))

(defun process-unexpected-frame (conn)
  (cl-rabbit::simple-wait-frame conn))

(defun find-free-channel-index (async-conn)
  (with-accessors ((channels async-connection/channels))
      async-conn
    (let ((pos (position nil channels)))
      (or pos
          (let ((old-size (length channels)))
            (adjust-array channels (* old-size 2) :initial-element nil)
            old-size)))))

(defun wait-for-frame (async-conn)
  (log:info "Waiting for frame on async connection: ~s" async-conn)
  (with-accessors ((channels async-connection/channels)
                   (conn async-connection/connection))
      async-conn
    ;;
    (handler-bind ((cl-rabbit:rabbitmq-library-error
                    (lambda (condition)
                      (when (eq (cl-rabbit:rabbitmq-library-error/error-code condition)
                                :amqp-unexpected-frame)
                        (log:info "Got unexpected frame")
                        (process-unexpected-frame conn)))))
      ;;
      (let ((envelope (cl-rabbit:consume-message conn)))
        (log:info "Got message: ~s" envelope)
        (let ((channel-id (cl-rabbit:envelope/channel envelope)))
          (labels ((warn-nonexistent-channel ()
                     (log:warn "Message received for closed channel: ~a" channel-id)))
            (if (< channel-id (length channels))
                (let ((async-channel (aref channels channel-id)))
                  (if async-channel
                      (dolist (callback (async-channel/message-callbacks async-channel))
                        (funcall callback envelope))
                      ;; ELSE: Unused channel
                      (warn-nonexistent-channel)))
                ;; ELSE: Message for unused channel
                (warn-nonexistent-channel))))))))

(defun handle-command (async-conn in-fd)
  (sb-alien:with-alien ((buf (sb-alien:array sb-alien:int 1)))
    (let* ((size (/ (sb-alien:alien-size sb-alien:int) 8))
           (result (sb-posix:read in-fd (sb-alien:addr buf) size)))
      (unless (= result size)
        (error "Unable to read from pipe"))
      (let ((index (sb-alien:deref buf 0)))
        (let ((cmd (bordeaux-threads:with-lock-held ((async-connection/lock async-conn))
                     (with-accessors ((requests async-connection/requests)) async-conn
                       (let ((cmd (assoc index requests)))
                         (unless cmd
                           (error "Attempt to get missing command index: ~s" index))
                         (setf requests (delete (car cmd) requests :key #'car))
                         (cdr cmd))))))
          (let ((result (funcall (first cmd))))
            (funcall (second cmd) result)))))))

(defun run-sync-loop (async-conn in-fd)
  (let ((socket-fd (cl-rabbit::get-sockfd (async-connection/connection async-conn))))
    (sb-alien:with-alien ((rfds (sb-alien:struct sb-unix:fd-set)))
      (sb-unix:fd-zero rfds)
      (sb-unix:fd-set socket-fd rfds)
      (sb-unix:fd-set in-fd rfds)
      (multiple-value-bind (count err)
          (sb-unix:unix-fast-select (1+ (max socket-fd in-fd)) (sb-alien:addr rfds) nil nil 10 0)
        (if count
            (progn
              (when (sb-unix:fd-isset in-fd rfds)
                (handle-command async-conn in-fd))
              (when (sb-unix:fd-isset socket-fd rfds)
                (wait-for-frame async-conn)))
            ;; ELSE: An error occurred in the call
            (unless (= err sb-unix:eintr)
              (error "Error: ~s" err)))))
    (log:info "Sync loop for fd ~a finished" socket-fd)))

(defun run-in-sync-thread (async-conn fn)
  (let* ((h (make-transfer-handler))
         (index (bordeaux-threads:with-lock-held ((async-connection/lock async-conn))
                  (let ((index (incf (async-connection/request-index async-conn))))
                    (push (list index fn (lambda (result)
                                           (set-value h result)))
                          (async-connection/requests async-conn))
                    index))))
    (sb-alien:with-alien ((buf (sb-alien:array sb-alien:int 1)))
      (setf (sb-alien:deref buf 0) index)
      (sb-posix:write (async-connection/cmd-fd async-conn)
                      (sb-alien:addr buf)
                      (/ (sb-alien:alien-size sb-alien:int) 8)))
    (wait-for-value h)))

(defun open-channel (async-conn)
  (run-in-sync-thread async-conn
                      (lambda ()
                        (let* ((index (find-free-channel-index async-conn))
                               (channel (1+ index)))
                          (cl-rabbit:channel-open (async-connection/connection async-conn) channel)
                          (log:info "Opened channel: ~s" index)
                          (let ((async-channel (make-instance 'async-channel
                                                              :connection async-conn
                                                              :channel channel)))
                            (setf (aref (async-connection/channels async-conn) index) async-channel)
                            async-channel)))))

(defun make-async-connection ()
  (let* ((conn (cl-rabbit:new-connection))
         (socket (cl-rabbit:tcp-socket-new conn)))
    (cl-rabbit:socket-open socket "localhost" 5672)
    (cl-rabbit:login-sasl-plain conn "/" "guest" "guest")
    (let ((conn-wrapper (make-instance 'async-connection :connection conn)))
      (trivial-garbage:finalize conn-wrapper
                                (lambda ()
                                  (log:warn "Reference to RabbitMQ connection object lost. Closing.")
                                  (cl-rabbit:destroy-connection conn)))
      (multiple-value-bind (in-fd out-fd)
          (sb-posix:pipe)
        (setf (async-connection/cmd-fd conn-wrapper) out-fd)
        (bordeaux-threads:make-thread (lambda () (run-sync-loop conn-wrapper in-fd))
                                      :name "RabbitMQ listener")
        conn-wrapper))))

#+nil(defun connect-test ()
  (cl-rabbit:with-connection (conn)
    (let ((socket (cl-rabbit:tcp-socket-new conn)))
      (cl-rabbit:socket-open socket "localhost" 5672)
      (cl-rabbit:login-sasl-plain conn "/" "guest" "guest")
      (cl-rabbit:channel-open conn 1)
      (let ((q (cl-rabbit:queue-declare conn 1 :exclusive t :auto-delete t)))
        (cl-rabbit:queue-bind conn 1 :queue q :exchange "test-ex" :routing-key "xx")
        (let ((ctag (cl-rabbit:basic-consume conn 1 q)))
          (log:info "Waiting for messages. Consume tag: ~s" ctag)
          (run-sync-loop conn))))))
