(in-package :cl-rabbit-async)

(defun run-sync-loop (conn)
  (loop
     repeat 3
     for frame = (cl-rabbit::simple-wait-frame conn)
     do (log:info "Got frame: ~s" frame)))

(defun connect-test ()
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
