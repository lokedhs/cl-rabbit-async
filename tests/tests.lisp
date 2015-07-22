(in-package :cl-rabbit-async.tests)

(declaim (optimize (speed 0) (safety 3) (debug 3)))

(defun make-random-string (n)
  (with-output-to-string (s)
    (loop
       repeat n
       do (write-char (code-char (+ (random (1+ (- (char-code #\z) (char-code #\a)))) (char-code #\a))) s))))

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
                     (condition-broadcast condvar))
                   (log:info "Sending messages")
                   (loop
                      repeat num-messages
                      for i from 0
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
