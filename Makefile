.PHONY: dev-binder .binder-image

BINDER_IMAGE?=eclairjs/binder

.binder-image:
	@docker build --rm -t $(BINDER_IMAGE) .

dev-binder: .binder-image
	@docker run --rm -it -p 8888:8888  \
		-v `pwd`:/home/main/notebooks \
		--workdir /home/main/notebooks $(BINDER_IMAGE) \
		/home/main/start-notebook.sh --ip=0.0.0.0
