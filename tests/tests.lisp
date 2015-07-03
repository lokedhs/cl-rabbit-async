(in-package :cl-rabbit-async.tests)

(declaim (optimize (speed 0) (safety 3) (debug 3)))

(fiveam:test connect-test
  (let ((conn (make-async-connection "localhost")))
    (fiveam:is (typep conn 'async-connection))
    (let ((channel (open-channel conn)))
      (fiveam:is (typep channel 'async-channel))
      (close-channel channel))
    (close-async-connection conn)))
