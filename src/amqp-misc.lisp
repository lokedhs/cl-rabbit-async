(in-package :cl-rabbit-async)

(declaim (optimize (safety 3) (debug 3) (speed 0)))

(defun simple-wait-frame (conn)
  (cl-rabbit::with-state (state conn)
    (cffi:with-foreign-objects ((decoded '(:struct cl-rabbit::amqp-frame-t)))
      (let* ((result (cl-rabbit::amqp-simple-wait-frame state decoded))
             (result-tag (cffi:foreign-enum-keyword 'cl-rabbit::amqp-status-enum result)))
        (log:info "Channel: ~a, type: ~a"
                  (cffi:foreign-slot-value decoded '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::channel)
                  (cffi:foreign-slot-value decoded '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::frame-type))
        (let ((frame-type (cffi:foreign-slot-value decoded '(:struct cl-rabbit::amqp-frame-t)
                                                   'cl-rabbit::frame-type)))
          (cond ((= frame-type cl-rabbit::amqp-frame-method)
                 (log:info "Method frame: ~s"
                           (cffi:foreign-slot-value decoded '(:struct cl-rabbit::amqp-frame-t)
                                                    'cl-rabbit::payload-method)))
                ((= frame-type cl-rabbit::amqp-frame-header)
                 (log:info "Header frame. Class id: ~a, Body size: ~a"
                           (cffi:foreign-slot-value decoded '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::payload-properties-class-id)
                           (cffi:foreign-slot-value decoded '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::payload-properties-body-size)))
                ((= frame-type cl-rabbit::amqp-frame-body)
                 (log:info "Body frame: ~s"
                           (cffi:foreign-slot-value decoded '(:struct cl-rabbit::amqp-frame-t) 'cl-rabbit::payload-body-fragment)))
                (t
                 (log:warn "Unknown frame type: ~s" frame-type))))
        result-tag))))

