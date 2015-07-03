(in-package :cl-rabbit-async.tests)

(declaim (optimize (speed 0) (safety 3) (debug 3)))

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
           (async-exchange-declare c1 "foo-ex" "topic" :durable t :auto-delete t)
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
