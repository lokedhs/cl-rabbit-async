(in-package :cl-rabbit-async.tests)

(declaim (optimize (speed 0) (safety 3) (debug 3)))

(defun make-random-string (n)
  (with-output-to-string (s)
    (loop
       repeat n
       do (write-char (code-char (+ (random (1+ (- (char-code #\z) (char-code #\a)))) (char-code #\a))) s))))

(defmacro with-async-connection (conn &body body)
  (let ((conn-sym (gensym "CONN-")))
    `(let ((,conn-sym (make-multi-connection "localhost" :max-channels 10)))
       (unwind-protect
            (let ((,conn ,conn-sym))
              ,@body)
        (progn
          (log:info "Closing mconn: ~s" ',conn)
          (close-multi-connection ,conn-sym))))))

(fiveam:test connect-test
  (let ((conn (make-async-connection "localhost")))
    (unwind-protect
         (progn
           (fiveam:is (typep conn 'async-connection))
           (let ((channel (open-channel conn)))
             (fiveam:is (typep channel 'async-channel))
             (close-channel channel)))
      (close-async-connection conn))))

(fiveam:test send-message-test
  (let ((conn (make-async-connection "localhost")))
    (unwind-protect
         (let* ((message-text "test message")
                (received-message nil)
                (c1 (open-channel conn :message-callback (lambda (msg) (setq received-message msg))))
                (c2 (open-channel conn)))
           ;; c1 is the receiver channel
           ;; c2 is the sender channel
           (async-exchange-declare c1 "foo-ex" "topic" :durable t)
           (let ((q (async-queue-declare c1 :exclusive t :auto-delete t)))
             (async-queue-bind c1 :queue q :exchange "foo-ex" :routing-key "#")
             (async-basic-consume c1 q :no-ack t)
             ;; Send the message on the other channel
             (async-basic-publish c2
                                  :exchange "foo-ex"
                                  :routing-key "foo"
                                  :body (babel:string-to-octets message-text))
             ;; Wait a second to allow the message to be deliviered
             (sleep 1)
             ;; Verify that the message has been received
             (fiveam:is-true received-message)
             (let ((body (cl-rabbit:message/body (cl-rabbit:envelope/message received-message))))
               (fiveam:is (equal (babel:octets-to-string body) message-text)))))
      (close-async-connection conn))))

(fiveam:test close-callback-test
  (let ((m 10)
        (lock (bordeaux-threads:make-lock "Channel counter lock"))
        (opened-channels nil)
        (failed-count 0)
        (conn (make-async-connection "localhost")))
    (unwind-protect
         (labels ((remove-from-channel-list (channel)
                    (bordeaux-threads:with-lock-held (lock)
                      (unless (member channel opened-channels)
                        (incf failed-count))
                      (setq opened-channels (remove channel opened-channels))))
                  (open-and-increment ()
                    (bordeaux-threads:with-lock-held (lock)
                      (push (open-channel conn
                                          :close-callback (lambda (channel)
                                                            (remove-from-channel-list channel)))
                            opened-channels))))
           (loop
              repeat m
              do (open-and-increment)))
      (fiveam:is (eql m (length opened-channels)))
      (fiveam:is (eql 0 failed-count))
      (close-async-connection conn)
      (sleep 1)
      (fiveam:is (null opened-channels)))))

(fiveam:test parallel-open-test
  (let ((conn (make-async-connection "localhost")))
    (unwind-protect
         (let ((lock (bordeaux-threads:make-lock))
               (condvar (bordeaux-threads:make-condition-variable))
               (enabled nil)
               (num-messages 10)
               (num-threads 10)
               (num-received 0)
               (errors nil))

           (labels ((push-error (msg)
                      (log:error "~a" msg)
                      (bordeaux-threads:with-lock-held (lock)
                        (push msg errors)))

                    (process-queue (q)
                      (bordeaux-threads:with-lock-held (lock)
                        (loop
                           until enabled
                           do (bordeaux-threads:condition-wait condvar lock)))
                      (let* ((messages nil)
                             (consumer-tag (make-random-string 60))
                             (inner-lock (bordeaux-threads:make-lock))
                             (inner-condvar (bordeaux-threads:make-condition-variable))
                             (ch (open-channel conn :message-callback (lambda (msg)
                                                                        (unless (equal (cl-rabbit:envelope/consumer-tag msg)
                                                                                       consumer-tag)
                                                                          (push-error "Consumer tag did not match"))
                                                                        (bordeaux-threads:with-lock-held (lock)
                                                                          (incf num-received))
                                                                        (bordeaux-threads:with-lock-held (inner-lock)
                                                                          (push msg messages)
                                                                          (bordeaux-threads:condition-notify inner-condvar))))))
                        (async-basic-consume ch q :no-ack t :consumer-tag consumer-tag)
                        (bordeaux-threads:with-lock-held (inner-lock)
                          (loop
                             until (= (length messages) num-messages)
                             do (bordeaux-threads:condition-wait inner-condvar inner-lock))))))

             (let ((ch (open-channel conn)))
               (async-exchange-declare ch "foo-ex" "topic" :durable t)
               (let ((queues (loop
                                repeat num-threads
                                for q = (async-queue-declare ch :durable nil :auto-delete t :exclusive nil)
                                do (async-queue-bind ch :queue q :exchange "foo-ex" :routing-key "#")
                                collect q)))
                 (log:info "Created queues: ~s" queues)
                 (let ((threads (loop
                                   for q in queues
                                   for i from 0
                                   collect (let ((queue-copy q))
                                             (bordeaux-threads:make-thread #'(lambda () (process-queue queue-copy))
                                                                           :name (format nil "Queue reader ~a, queue: ~a" i q))))))
                   (sleep 1)
                   (bordeaux-threads:with-lock-held (lock)
                     (setq enabled t)
                     (cl-rabbit-async::condition-broadcast condvar))
                   (log:info "Sending messages")
                   (loop
                      for i from 0 below num-messages
                      do (async-basic-publish ch
                                              :exchange "foo-ex"
                                              :routing-key "foo"
                                              :body (format nil "Message number: ~a" i)))
                   (log:info "Waiting for threads")
                   (loop
                      for thread in threads
                      do (bordeaux-threads:join-thread thread))
                   (dolist (err errors)
                     (fiveam:fail err))
                   (fiveam:is (= (* num-messages num-threads) num-received)))))))

      ;; Unwind form
      (close-async-connection conn))))

(fiveam:test open-close-test
  (let ((lock (bordeaux-threads:make-lock))
        (stopped nil)
        (num-readers 10)
        (errors nil)
        (recv-count 0))
    (with-async-connection conn
      (labels ((push-error (message &optional condition)
                 (bordeaux-threads:with-lock-held (lock)
                   (let ((text (if condition
                                   (format nil "Error: ~a: ~a" message condition)
                                   (format nil "Error: ~a" message))))
                     (log:error "~a" text)
                     (push text errors))))

               (stopped-p ()
                 (bordeaux-threads:with-lock-held (lock)
                   stopped))

               (provider-loop ()
                 (with-async-connection provider-conn
                   (let ((channel (multi-connection-open-channel provider-conn)))
                     (loop
                        for i from 0
                        until (stopped-p)
                        do (progn
                             (loop
                                for reader-index from 0 below num-readers
                                do (async-basic-publish channel
                                                        :exchange "foo-ex"
                                                        :routing-key (format nil "r~a.i~a" reader-index i)
                                                        :body (format nil "iteration: ~a, reader ~a"
                                                                      i reader-index)))
                             (sleep 1/10))))))

               (new-msg (msg ctag)
                 (cond ((null ctag)
                        (push-error "Null ctag"))
                       ((not (equal (cl-rabbit:envelope/consumer-tag msg) ctag))
                        (push-error (format nil "Unexpected ctag, got: ~a, expected: ~a"
                                            (cl-rabbit:envelope/consumer-tag msg) ctag)))
                       (t
                        (bordeaux-threads:with-lock-held (lock)
                          (incf recv-count)))))

               (reader-loop (reader-index)
                 (let ((queue (let* ((channel (multi-connection-open-channel conn))
                                     (q (async-queue-declare channel :durable t :arguments '(("x-expires" . 5000)))))
                                (async-queue-bind channel
                                                  :queue q
                                                  :exchange "foo-ex"
                                                  :routing-key (format nil "r~a.*" reader-index))
                                (close-channel channel)
                                q)))
                   (loop
                      until (stopped-p)
                      do (let* ((ctag nil)
                                (reader-lock (bordeaux-threads:make-lock))
                                (channel (multi-connection-open-channel conn :message-callback (lambda (msg)
                                                                                                 (let ((consumer-tag (bordeaux-threads:with-lock-held (reader-lock)
                                                                                                                       ctag)))
                                                                                                   (new-msg msg consumer-tag))))))
                           (bordeaux-threads:with-lock-held (reader-lock)
                             (let ((consumer-tag (async-basic-consume channel queue :no-ack t)))
                               (setf ctag consumer-tag)))
                           (sleep 1)
                           (close-channel channel))))))

        (let ((channel (multi-connection-open-channel conn)))
          (async-exchange-declare channel "foo-ex" "topic" :durable t)
          (close-channel channel))

        (let ((provider-thread (bordeaux-threads:make-thread (lambda ()
                                                               (handler-case
                                                                   (provider-loop)
                                                                 (error (condition)
                                                                   (push-error "Provider thread" condition))))))
              (threads (loop
                          for reader-index from 0 below num-readers
                          collect (bordeaux-threads:make-thread (lambda ()
                                                                  (block read-loop-handler
                                                                   (handler-bind ((error (lambda (condition)
                                                                                           (push-error (format nil "Reader ~a: ~a" reader-index
                                                                                                               condition))
                                                                                           (return-from read-loop-handler nil))))
                                                                     (reader-loop reader-index))))))))
          (sleep 20)
          (bordeaux-threads:with-lock-held (lock)
            (setq stopped t))
          (bordeaux-threads:join-thread provider-thread)
          (mapc #'bordeaux-threads:join-thread threads)
          (log:info "recv-count: ~a" recv-count)
          (fiveam:is (null errors)))))))

(defun num-conn-channels (mconn)
  (loop
     for conn in (cl-rabbit-async::multi-connection/connections mconn)
     summing (cl-rabbit-async::num-opened-channels (cl-rabbit-async::mconnection-wrapper/connection conn))))

(fiveam:test multi-conn-test
  (with-async-connection conn
    (labels ((open-channels (n)
               (loop
                  for i from 1 to n
                  collect (multi-connection-open-channel conn)
                  do (fiveam:is (= i (num-conn-channels conn)))))
             (close-channels (channels n)
               (let ((total-channel-count (length channels)))
                 (when (< total-channel-count n)
                   (error "Attempt to close more channels than were opened"))
                 (loop
                    with num-channels = total-channel-count
                    repeat n
                    for ch in channels
                    do (progn
                         (close-channel ch)
                         (decf num-channels)
                         (fiveam:is (= num-channels (num-conn-channels conn))))))))
      (let ((channels (open-channels 20)))
        (close-channels channels (length channels)))
      (loop
         repeat 20
         do (let ((channels (open-channels 1)))
              (close-channels channels 1))))))
